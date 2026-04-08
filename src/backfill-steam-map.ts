/**
 * Backfill steam_map from HappyView's Postgres.
 *
 * Queries all game records that have both an IGDB ID and a Steam ID,
 * then populates the steam_map table in state.sqlite.
 *
 * Usage: npx tsx src/backfill-steam-map.ts
 */

import 'dotenv/config'
import pg from 'postgres'
import { StateManager } from './state.js'

async function main() {
	const dbUrl = process.env.HAPPYVIEW_DATABASE_URL
	if (!dbUrl) {
		throw new Error('Missing HAPPYVIEW_DATABASE_URL')
	}

	const sql = pg(dbUrl)
	const state = new StateManager()

	console.log('[backfill-steam-map] Querying game records with Steam IDs from HappyView...')
	const rows = await sql`
		SELECT uri, record
		FROM records
		WHERE collection = 'games.gamesgamesgamesgames.game'
	`
	console.log(`[backfill-steam-map] Found ${rows.length} game records, filtering for Steam IDs...`)

	let count = 0
	for (const row of rows) {
		const rec = typeof row.record === 'string' ? JSON.parse(row.record) : row.record
		const igdbId = rec?.externalIds?.igdb
		const steamId = rec?.externalIds?.steam
		if (!igdbId || !steamId) continue
		state.setSteamMapping(igdbId, steamId, row.uri)
		count++
		if (count % 10000 === 0) {
			console.log(`[backfill-steam-map] Progress: ${count}/${rows.length}`)
		}
	}

	console.log(`[backfill-steam-map] Done. ${count} steam_map entries written.`)

	await sql.end()
	state.close()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
