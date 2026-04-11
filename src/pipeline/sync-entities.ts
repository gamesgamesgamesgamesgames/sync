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
}

export interface SyncResult {
	created: number
	updated: number
	skipped: number
	failed: number
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

	const result: SyncResult = { created: 0, updated: 0, skipped: 0, failed: 0 }
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
			items.map((item) =>
				pool.run(async () => {
					const key = getKey(item)
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
				}),
			),
		)

		for (const entry of mapped) {
			if (entry.status === 'rejected') {
				console.error(`  [!] ${errorLabel(entry.reason)} ${entityType} sync failed:`, (entry.reason as Error).message)
				result.failed++
				continue
			}

			const { key, record, existingUri, name, slug } = entry.value

			if (existingUri) {
				// Full scan: skip existing records entirely (they haven't changed
				// since they were written — IGDB's updated_at filter handles
				// incremental changes on future runs).
				if (!lastSyncAt || !record) {
					result.skipped++
					continue
				}

				// Incremental sync: fetch and diff before updating.
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
						result.failed++
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

	state.setLastSyncAt(entityType, syncStartedAt)
	console.log(`[sync] ${entityType}: done (created=${result.created}, updated=${result.updated}, skipped=${result.skipped}, failed=${result.failed})`)
	return result
}
