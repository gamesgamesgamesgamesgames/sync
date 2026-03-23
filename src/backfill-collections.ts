/**
 * Backfill collection records with their `games` arrays.
 *
 * Reads IGDB collections and franchises, looks up the matching AT URI
 * and existing record from HappyView's Postgres (by name + type), maps
 * IGDB game IDs to AT URIs via state.sqlite, then batch-updates the
 * collection records on the PDS with the `games` array.
 *
 * Tracks progress in state.sqlite so it can resume after a crash.
 * Falls back to individual writes if a batch write fails (413).
 *
 * Usage:
 *   npx tsx src/backfill-collections.ts
 */

import 'dotenv/config'
import Database from 'better-sqlite3'
import postgres from 'postgres'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import type { IGDBCollection, IGDBFranchise } from './igdb/types.js'

const COLLECTION_NSID = 'games.gamesgamesgamesgames.collection'
const HAPPYVIEW_URL = process.env.HAPPYVIEW_DATABASE_URL!
const BATCH_SIZE = 25

interface CollectionInfo {
	uri: string
	record: Record<string, unknown>
}

interface UpdateEntry {
	collection: string
	rkey: string
	record: Record<string, unknown>
	name: string
	key: string
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

	// Progress tracking in state.sqlite
	const progressDb = state.getDb()
	progressDb.exec(`
		CREATE TABLE IF NOT EXISTS collection_backfill_done (
			key TEXT PRIMARY KEY
		)
	`)
	const isDone = progressDb.prepare('SELECT 1 FROM collection_backfill_done WHERE key = ?')
	const markDone = progressDb.prepare('INSERT OR IGNORE INTO collection_backfill_done (key) VALUES (?)')

	// Connect to HappyView Postgres to look up collection URIs and records
	const sql = postgres(HAPPYVIEW_URL)

	// Build a lookup map: "name|type" → { uri, record } from HappyView
	console.log('Loading collection records from HappyView...')
	const collectionRows = await sql`
		SELECT uri, record
		FROM records
		WHERE collection = ${COLLECTION_NSID}
	`
	const collectionsByKey = new Map<string, CollectionInfo>()
	for (const row of collectionRows) {
		const rec = row.record as Record<string, unknown>
		const name = rec.name as string | undefined
		const type = rec.type as string | undefined
		if (name && type) {
			collectionsByKey.set(`${name}|${type}`, { uri: row.uri, record: rec })
		}
	}
	console.log(`  Loaded ${collectionsByKey.size} collections from HappyView.`)

	const shutdown = () => {
		console.log('\n[shutdown] Closing...')
		state.close()
		sql.end()
		process.exit(0)
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	console.log()
	console.log('=== Collections Backfill ===')
	console.log()

	let updated = 0
	let skipped = 0
	let noGames = 0
	let alreadyDone = 0
	let errors = 0

	// Pending batch of updates
	let batch: UpdateEntry[] = []

	async function flushBatch() {
		if (batch.length === 0) return

		try {
			await atproto.applyUpdates(batch.map((e) => ({
				collection: e.collection,
				rkey: e.rkey,
				record: e.record,
			})))
			// Mark all as done
			for (const entry of batch) {
				markDone.run(entry.key)
			}
			updated += batch.length
			console.log(`  [batch] Updated ${batch.length} collections (${updated} total) — last: ${batch[batch.length - 1]!.name}`)
		} catch (err) {
			// Fallback to individual writes
			console.warn(`  [!] Batch failed (${(err as Error).message}), falling back to individual writes...`)
			for (const entry of batch) {
				try {
					await atproto.putRecord(entry.collection, entry.rkey, entry.record)
					markDone.run(entry.key)
					updated++
					console.log(`  [+] ${entry.name}`)
				} catch (innerErr) {
					console.error(`  [!] Failed: ${entry.name}: ${(innerErr as Error).message}`)
					errors++
				}
			}
		}

		batch = []
	}

	// Phase 1: IGDB Collections → type: "series"
	console.log('[backfill] Processing IGDB collections...')
	for await (const { items } of igdb.paginate<IGDBCollection>(
		'collections',
		'fields name, slug, games;',
		0,
	)) {
		for (const collection of items) {
			const key = `c_${collection.id}`
			if (isDone.get(key)) { alreadyDone++; continue }

			const result = prepareUpdate(key, collection.name, 'series', collection.games, collectionsByKey, state)
			if (result === 'skipped') { skipped++; continue }
			if (result === 'no-games') { noGames++; continue }
			batch.push(result)
			if (batch.length >= BATCH_SIZE) await flushBatch()
		}
	}
	await flushBatch()

	// Phase 2: IGDB Franchises → type: "franchise"
	console.log('[backfill] Processing IGDB franchises...')
	for await (const { items } of igdb.paginate<IGDBFranchise>(
		'franchises',
		'fields name, slug, games;',
		0,
	)) {
		for (const franchise of items) {
			const key = `f_${franchise.id}`
			if (isDone.get(key)) { alreadyDone++; continue }

			const result = prepareUpdate(key, franchise.name, 'franchise', franchise.games, collectionsByKey, state)
			if (result === 'skipped') { skipped++; continue }
			if (result === 'no-games') { noGames++; continue }
			batch.push(result)
			if (batch.length >= BATCH_SIZE) await flushBatch()
		}
	}
	await flushBatch()

	console.log()
	console.log('=== Backfill complete ===')
	console.log(`  Updated: ${updated}`)
	console.log(`  Already done (resumed): ${alreadyDone}`)
	console.log(`  Skipped (no match in HappyView): ${skipped}`)
	console.log(`  No games: ${noGames}`)
	console.log(`  Errors: ${errors}`)

	state.close()
	await sql.end()
}

function prepareUpdate(
	key: string,
	name: string,
	type: string,
	igdbGameIds: number[] | undefined,
	collectionsByKey: Map<string, CollectionInfo>,
	state: StateManager,
): UpdateEntry | 'skipped' | 'no-games' {
	const info = collectionsByKey.get(`${name}|${type}`)
	if (!info) return 'skipped'

	if (!igdbGameIds || igdbGameIds.length === 0) return 'no-games'

	// Map IGDB game IDs to AT URIs
	const gameUris: string[] = []
	for (const gameId of igdbGameIds) {
		const gameUri = state.getEntity('game', gameId)
		if (gameUri) gameUris.push(gameUri)
	}

	if (gameUris.length === 0) return 'no-games'

	// Extract the rkey from the collection URI
	const rkey = info.uri.split('/').pop()!

	// Merge games into the existing record
	const record = { ...info.record, games: gameUris }
	delete record.$type

	return { collection: COLLECTION_NSID, rkey, record, name, key }
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
