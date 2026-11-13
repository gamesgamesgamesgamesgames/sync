/**
 * Generic incremental sync for all entity types.
 *
 * Uses IGDB's `updated_at` field to fetch only entities that have
 * changed since the last sync. New entities are batch-created,
 * existing entities are updated via putRecord.
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager, EntityType } from '../state.js'
import { XRPCError } from '@atproto/xrpc'
import { ContributionRequestError, type ContributionClient } from '../contributions/client.js'
import { syncGameViaContribution } from '../contributions/handler.js'
import { errorLabel, prefetch } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'
import { ConcurrencyPool } from '../concurrency.js'

export interface SyncEntityConfig<T extends { id: number }> {
	entityType: EntityType
	igdbEndpoint: string
	igdbFields: string
	collection: string
	mapRecord: (item: T) => Promise<Record<string, unknown>>
	/** How to derive the state key from the IGDB item (default: String(item.id)) */
	getStateKey?: (item: T) => string
	/** Update batch size — default 200, use 10 for games (large records hit 2MB limit) */
	updateBatchSize?: number
	/** Number of concurrent mapRecord calls per page — default 1 (sequential) */
	concurrency?: number
	/** IGDB pagination offset to start from — skips ahead in the full scan */
	startOffset?: number
	/** Return the slug for this item, if any. When set, the slug is written via HappyView's putGame XRPC. */
	getSlug?: (item: T) => string | undefined
	/** HappyView base URL for XRPC calls (e.g. slug writes via putGame). Required when getSlug is set. */
	happyviewUrl?: string
	/** HappyView API key for authenticating XRPC calls. */
	happyviewApiKey?: string
	/** When set, games are synced via the contribution path instead of direct writes. */
	contributionClient?: ContributionClient
	/** Consecutive failed runs before an entity stops holding the cursor (default SYNC_MAX_FAILED_RUNS or 3). */
	maxFailedRuns?: number
	/** Warn when the cursor is older than this many hours (default SYNC_CURSOR_ALERT_HOURS or 72). */
	cursorAlertHours?: number
}

export interface SyncResult {
	created: number
	updated: number
	skipped: number
	failed: number
	/** Failures holding the cursor back this run. */
	retryable: number
	/** Entities moved to `failures` this run after too many failed runs. */
	givenUp: number
	/** Whether the incremental-sync cursor moved forward. */
	cursorAdvanced: boolean
}

/**
 * How a failed entity affects the incremental-sync cursor.
 *
 * - `permanent`: HappyView refused the request itself (a 4xx other than
 *   auth, timeout or rate limit). Recorded in `failures`; the cursor moves on.
 * - `systemic`: auth, rate limits and network errors. They hit every entity
 *   alike, so they always hold the cursor and never count against one entity.
 * - `entity`: anything else, including 5xx (HappyView 2.17.5 answers every
 *   Lua error() with 500, so a script refusing one game looks like this) and
 *   errors from our own code. They hold the cursor for up to
 *   SYNC_MAX_FAILED_RUNS consecutive runs, then the entity moves to
 *   `failures` so one bad game cannot pin the cursor forever.
 */
export type FailureKind = 'permanent' | 'systemic' | 'entity'

const SYSTEMIC_STATUSES = [401, 403, 408, 429]

export function classifyFailure(err: unknown): FailureKind {
	if (err instanceof ContributionRequestError || err instanceof XRPCError) {
		const status = err.status
		if (SYSTEMIC_STATUSES.includes(status)) return 'systemic'
		if (status >= 400 && status < 500) return 'permanent'
		return 'entity'
	}
	// fetch() reports connection failures as a TypeError("fetch failed").
	if (err instanceof TypeError && /fetch failed|ECONNRESET|ETIMEDOUT|ENOTFOUND|socket/i.test(err.message)) {
		return 'systemic'
	}
	return 'entity'
}

/** Kept for callers that only need the yes/no answer. */
export function isPermanentFailure(err: unknown): boolean {
	return classifyFailure(err) === 'permanent'
}

/** A mapRecord failure, carrying the key of the entity it was mapping. */
class KeyedFailure extends Error {
	constructor(readonly key: string, readonly cause: unknown) {
		super((cause as Error)?.message ?? String(cause))
	}
}

function envNumber(name: string, fallback: number): number {
	const value = Number(process.env[name])
	return Number.isFinite(value) && value > 0 ? value : fallback
}

/** Deep-compare two records, ignoring the `$type` field added by the PDS. */
function recordsEqual(
	existing: Record<string, unknown>,
	incoming: Record<string, unknown>,
	collection: string,
): boolean {
	// Normalize: add $type to incoming so both sides match
	const normalized = { $type: collection, ...incoming }
	return JSON.stringify(sortKeys(existing)) === JSON.stringify(sortKeys(normalized))
}

function sortKeys(obj: unknown): unknown {
	if (obj === null || typeof obj !== 'object') return obj
	if (Array.isArray(obj)) return obj.map(sortKeys)
	const sorted: Record<string, unknown> = {}
	for (const key of Object.keys(obj as Record<string, unknown>).sort()) {
		sorted[key] = sortKeys((obj as Record<string, unknown>)[key])
	}
	return sorted
}

async function putSlugViaXrpc(happyviewUrl: string, apiKey: string, uri: string, slug: string): Promise<void> {
	const response = await fetch(`${happyviewUrl}/xrpc/games.gamesgamesgamesgames.putGame`, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'Authorization': `Bearer ${apiKey}`,
		},
		body: JSON.stringify({ uri, slug }),
	})
	if (!response.ok) {
		const body = await response.text()
		throw new Error(`putGame returned ${response.status}: ${body}`)
	}
}

export async function syncEntityType<T extends { id: number }>(
	config: SyncEntityConfig<T>,
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<SyncResult> {
	const { entityType, igdbEndpoint, igdbFields, collection } = config
	const getKey = config.getStateKey ?? ((item: T) => String(item.id))
	const updateBatchSize = config.updateBatchSize ?? 200
	const concurrency = config.concurrency ?? 1
	const startOffset = config.startOffset ?? 0

	const syncStartedAt = Math.floor(Date.now() / 1000)
	const lastSyncAt = state.getLastSyncAt(entityType)

	let where = ''
	if (lastSyncAt) {
		where = `where updated_at > ${lastSyncAt};`
		console.log(`[sync] ${entityType}: incremental sync (changes since ${new Date(lastSyncAt * 1000).toISOString()})`)
	} else {
		console.log(`[sync] ${entityType}: full scan (no previous sync timestamp)`)
	}

	const result: SyncResult = { created: 0, updated: 0, skipped: 0, failed: 0, retryable: 0, givenUp: 0, cursorAdvanced: false }
	const maxFailedRuns = config.maxFailedRuns ?? envNumber('SYNC_MAX_FAILED_RUNS', 3)
	const cursorAlertHours = config.cursorAlertHours ?? envNumber('SYNC_CURSOR_ALERT_HOURS', 72)

	if (lastSyncAt) {
		const ageHours = (syncStartedAt - lastSyncAt) / 3600
		if (ageHours > cursorAlertHours) {
			console.warn(`[sync] [ALERT] ${entityType}: the cursor is ${Math.round(ageHours)}h old (limit ${cursorAlertHours}h). Something has held it back for several runs; check the retryable failures below.`)
		}
	}

	/** Record a failure and decide whether it holds the cursor back. */
	const recordFailure = (key: string | undefined, err: unknown) => {
		result.failed++
		const kind = classifyFailure(err)
		if (kind === 'permanent') {
			if (key) state.addFailure(entityType, key)
			return
		}
		if (kind === 'entity' && key) {
			const runs = state.recordFailedRun(entityType, key, syncStartedAt, (err as Error)?.message ?? String(err))
			if (runs >= maxFailedRuns) {
				console.warn(`  [!] ${entityType} ${key} has failed ${runs} runs in a row; moving it to failures so it stops holding the cursor`)
				state.addFailure(entityType, key)
				state.clearFailureStreak(entityType, key)
				result.givenUp++
				return
			}
		}
		result.retryable++
	}
	const pool = new ConcurrencyPool(concurrency)
	let totalProcessed = 0

	if (startOffset > 0) {
		console.log(`[sync] ${entityType}: starting at offset ${startOffset}`)
	}

	for await (const { items, offset } of prefetch(igdb.paginate<T>(igdbEndpoint, igdbFields, startOffset, where))) {
		const batchStart = Date.now()
		console.log(`  [sync] ${entityType}: got ${items.length} items at offset ${offset}`)

		const createBatch: PendingRecord[] = []
		const updateBatch: Array<{ key: string; rkey: string; record: Record<string, unknown>; name: string; slug?: string }> = []

		const mapped = await Promise.allSettled(
			items.map((item) => {
				const itemKey = getKey(item)
				return pool.run(async () => {
					const key = itemKey
					const existingUri = state.getEntity(entityType, key)
					totalProcessed++
					if (totalProcessed % 50 === 0) {
						console.log(`  [sync] ${entityType}: processed ${totalProcessed} (current: ${key})`)
					}

					const slug = config.getSlug?.(item)

					// Full scan: skip mapRecord entirely for existing records
					if (!lastSyncAt && existingUri) {
						return { key, record: null, existingUri, name: key, slug }
					}

					const record = await config.mapRecord(item)
					return { key, record, existingUri, name: (record.name as string) ?? key, slug }
				}).catch((err) => {
					throw new KeyedFailure(itemKey, err)
				})
			}),
		)

		for (const entry of mapped) {
			if (entry.status === 'rejected') {
				const reason = entry.reason instanceof KeyedFailure ? entry.reason.cause : entry.reason
				const failedKey = entry.reason instanceof KeyedFailure ? entry.reason.key : undefined
				console.error(`  [!] ${errorLabel(reason)} ${entityType} sync failed:`, (reason as Error)?.message)
				recordFailure(failedKey, reason)
				continue
			}

			const { key, record, existingUri, name, slug } = entry.value

			// Contribution path: all games go through contributions
			if (config.contributionClient && record) {
				try {
					const syncResult = await syncGameViaContribution(key, record, config.contributionClient)

					state.clearFailureStreak(entityType, key)
					if (syncResult.action === 'created') {
						console.log(`  [contributions] ${syncResult.contributionType}: ${name} (${syncResult.uri})`)
						result.created++
					} else {
						result.skipped++
					}
				} catch (err) {
					console.error(`  [!] Contribution failed for ${name}:`, (err as Error).message)
					recordFailure(key, err)
				}
				continue
			}

			// Direct write path (non-game entities) — existing logic unchanged
			if (existingUri) {
				if (!lastSyncAt || !record) {
					result.skipped++
					continue
				}

				const rkey = existingUri.split('/').pop()!
				const existing = await atproto.getRecord(collection, rkey)
				if (existing && recordsEqual(existing, record, collection)) {
					result.skipped++
					continue
				}

				updateBatch.push({ key, rkey, record, name, slug })
			} else {
				createBatch.push({
					id: key,
					entityType,
					record: record!,
					collection,
					name: `${entityType}: ${name}`,
					slug,
				})
			}
		}

		// Flush updates
		for (let i = 0; i < updateBatch.length; i += updateBatchSize) {
			const chunk = updateBatch.slice(i, i + updateBatchSize)
			try {
				const results = await atproto.applyUpdates(
					chunk.map((u) => ({ collection, rkey: u.rkey, record: u.record })),
				)
				for (let j = 0; j < chunk.length; j++) {
					const entry = chunk[j]!
					const res = results[j]
					if (res) {
						state.setEntity(entityType, entry.key, res.uri)
						state.clearFailureStreak(entityType, entry.key)
						console.log(`  [sync] Updated: ${entry.name} (${res.uri})`)
						const steamId = (entry.record.externalIds as Record<string, unknown> | undefined)?.steam as string | undefined
						if (steamId) {
							state.setSteamMapping(entry.key, steamId, res.uri)
						}
						if (entry.slug && config.happyviewUrl && config.happyviewApiKey) {
							try {
								await putSlugViaXrpc(config.happyviewUrl, config.happyviewApiKey!, res.uri, entry.slug)
							} catch (err) {
								console.error(`  [!] Slug insert failed for ${entry.name}:`, (err as Error).message)
							}
						}
					}
				}
				result.updated += chunk.length
			} catch (err) {
				console.warn(`  [!] Update batch of ${chunk.length} rejected (${(err as Error).message}), falling back to individual puts`)
				for (const entry of chunk) {
					try {
						const { uri } = await atproto.putRecord(collection, entry.rkey, entry.record)
						state.setEntity(entityType, entry.key, uri)
						state.clearFailureStreak(entityType, entry.key)
						result.updated++
						console.log(`  [sync] Updated: ${entry.name} (${uri})`)
						const steamId = (entry.record.externalIds as Record<string, unknown> | undefined)?.steam as string | undefined
						if (steamId) {
							state.setSteamMapping(entry.key, steamId, uri)
						}
						if (entry.slug && config.happyviewUrl && config.happyviewApiKey) {
							try {
								await putSlugViaXrpc(config.happyviewUrl, config.happyviewApiKey!, uri, entry.slug)
							} catch (err) {
								console.error(`  [!] Slug insert failed for ${entry.name}:`, (err as Error).message)
							}
						}
					} catch (putErr) {
						console.error(`  [!] ${errorLabel(putErr)} Update failed: ${entry.name}:`, (putErr as Error).message)
						recordFailure(entry.key, putErr)
					}
				}
			}
		}

		// Flush creates
		if (createBatch.length > 0) {
			for (let i = 0; i < createBatch.length; i += 20) {
				const chunk = createBatch.slice(i, i + 20)
				await flushBatch(chunk, atproto, state)
			}
			result.created += createBatch.length

			// Populate steam_map for newly created games
			for (const entry of createBatch) {
				const steamId = (entry.record.externalIds as Record<string, unknown> | undefined)?.steam as string | undefined
				if (steamId) {
					const atUri = state.getEntity(entityType, entry.id)
					if (atUri) {
						state.setSteamMapping(String(entry.id), steamId, atUri)
					}
				}
			}

			if (config.getSlug && config.happyviewUrl && config.happyviewApiKey) {
				for (const entry of createBatch) {
					if (entry.slug) {
						const entityUri = state.getEntity(entityType, entry.id)
						if (entityUri) {
							try {
								await putSlugViaXrpc(config.happyviewUrl, config.happyviewApiKey!, entityUri, entry.slug)
							} catch (err) {
								console.error(`  [!] Slug insert failed for ${entry.name}:`, (err as Error).message)
							}
						}
					}
				}
			}
		}

		const batchElapsed = ((Date.now() - batchStart) / 1000).toFixed(1)
		console.log(`  [sync] ${entityType}: offset ${offset + items.length} (batch took ${batchElapsed}s)`)
	}

	// Moving the cursor past a failure drops that entity until IGDB touches it
	// again, so only move it when nothing failed in a way a rerun could fix.
	if (result.retryable === 0) {
		state.setLastSyncAt(entityType, syncStartedAt)
		result.cursorAdvanced = true
	} else {
		console.warn(`[sync] ${entityType}: ${result.retryable} retryable failure(s); keeping the cursor at ${lastSyncAt ? new Date(lastSyncAt * 1000).toISOString() : 'none'} so the next run retries them`)
	}
	console.log(`[sync] ${entityType}: done (created=${result.created}, updated=${result.updated}, skipped=${result.skipped}, failed=${result.failed}, retryable=${result.retryable}, givenUp=${result.givenUp})`)
	return result
}
