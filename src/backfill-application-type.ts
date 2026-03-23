/**
 * Backfill applicationType for all games.
 *
 * IGDB deprecated the numeric `category` field and replaced it with
 * `game_type` (a sub-resource with string `type`). Our scraper only
 * read `category`, which IGDB no longer populates — so all synced
 * games got `applicationType: 'game'` regardless of actual type.
 *
 * This script fetches the correct `game_type.type` from IGDB for
 * every game, compares it against the existing PDS record, and
 * writes back any that differ.
 *
 * Usage:
 *   npx tsx src/backfill-application-type.ts
 */

import 'dotenv/config'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { ConcurrencyPool } from './concurrency.js'
import { IGDB_GAME_TYPE_MAP } from './igdb/types.js'

const COLLECTION = 'games.gamesgamesgamesgames.game'
const OFFSET_KEY = 'backfill_app_type_offset'
const BATCH_SIZE = 20

interface Stats {
	processed: number
	updated: number
	skipped: number
	notInState: number
	errors: number
}

async function main() {
	const {
		TWITCH_CLIENT_ID,
		TWITCH_CLIENT_SECRET,
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
	} = process.env

	if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
		throw new Error('Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET in .env')
	}
	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD in .env')
	}

	const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
	await igdb.authenticate()

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	const state = new StateManager()
	const pool = new ConcurrencyPool(BATCH_SIZE)

	let shuttingDown = false
	const shutdown = () => {
		if (shuttingDown) return
		shuttingDown = true
		console.log('\n[shutdown] Graceful shutdown requested, finishing current page...')
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	const startOffset = state.getMetaNumber(OFFSET_KEY) ?? 0
	const stats: Stats = { processed: 0, updated: 0, skipped: 0, notInState: 0, errors: 0 }

	console.log('=== Backfill applicationType ===')
	console.log(`Resuming from offset ${startOffset}`)
	console.log()

	const pages = igdb.paginate<{ id: number; name: string; game_type?: { id: number; type?: string } }>(
		'games',
		'fields name, game_type.type;',
		startOffset,
	)

	for await (const { items, offset } of pages) {
		if (shuttingDown) break

		const updateBatch: Array<{ collection: string; rkey: string; record: Record<string, unknown> }> = []

		const tasks = items.map((game) =>
			pool.run(async () => {
				stats.processed++

				// Look up AT-URI in state
				const uri = state.getEntity('game', game.id)
				if (!uri) {
					stats.notInState++
					return
				}

				// Compute new applicationType from game_type
				const gameType = typeof game.game_type === 'object' ? game.game_type : undefined
				if (!gameType?.type) {
					stats.skipped++
					return
				}

				const newAppType = IGDB_GAME_TYPE_MAP[gameType.type]
				if (!newAppType) {
					stats.skipped++
					return
				}

				// Fetch existing record from PDS
				const rkey = uri.split('/').pop()!
				let existing: Record<string, unknown> | null
				try {
					existing = await atproto.getRecord(COLLECTION, rkey)
				} catch (err) {
					console.error(`[error] Failed to fetch record for IGDB ${game.id} (rkey ${rkey}):`, (err as Error).message)
					stats.errors++
					return
				}

				if (!existing) {
					stats.skipped++
					return
				}

				// Skip if already correct
				if (existing.applicationType === newAppType) {
					stats.skipped++
					return
				}

				// Queue update
				const { $type: _$type, ...record } = existing
				updateBatch.push({
					collection: COLLECTION,
					rkey,
					record: { ...record, applicationType: newAppType },
				})
			}),
		)

		await Promise.all(tasks)

		// Flush updates
		if (updateBatch.length > 0) {
			for (let i = 0; i < updateBatch.length; i += BATCH_SIZE) {
				const chunk = updateBatch.slice(i, i + BATCH_SIZE)
				try {
					await atproto.applyUpdates(chunk)
					stats.updated += chunk.length
				} catch {
					// Fallback to individual putRecord on batch failure
					console.warn(`[warn] Batch update failed, falling back to individual writes`)
					for (const write of chunk) {
						try {
							await atproto.putRecord(COLLECTION, write.rkey, write.record)
							stats.updated++
						} catch (err) {
							console.error(`[error] putRecord failed for rkey ${write.rkey}:`, (err as Error).message)
							stats.errors++
						}
					}
				}
			}
		}

		// Save offset after each page
		const nextOffset = offset + items.length
		state.setMeta(OFFSET_KEY, String(nextOffset))

		console.log(
			`[progress] offset=${nextOffset} | processed=${stats.processed} updated=${stats.updated} skipped=${stats.skipped} notInState=${stats.notInState} errors=${stats.errors}`,
		)
	}

	console.log()
	console.log('=== Backfill complete ===')
	console.log(`  Processed: ${stats.processed}`)
	console.log(`  Updated:   ${stats.updated}`)
	console.log(`  Skipped:   ${stats.skipped}`)
	console.log(`  Not in state: ${stats.notInState}`)
	console.log(`  Errors:    ${stats.errors}`)

	state.close()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
