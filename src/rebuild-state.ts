/**
 * Rebuild state.sqlite entities from HappyView's Postgres.
 *
 * Queries all game and platform records from HappyView and populates
 * the entities table so state is in sync with the PDS.
 *
 * Usage: HAPPYVIEW_DATABASE_URL=... npx tsx src/rebuild-state.ts
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

	// Rebuild games
	console.log('[rebuild] Querying game records from HappyView...')
	const gameRows = await sql`
		SELECT uri, record->'externalIds'->>'igdb' AS igdb_id
		FROM records
		WHERE collection = 'games.gamesgamesgamesgames.game'
		  AND record->'externalIds'->>'igdb' IS NOT NULL
	`
	console.log(`[rebuild] Found ${gameRows.length} game records`)

	let gameCount = 0
	for (const row of gameRows) {
		state.setEntity('game', row.igdb_id, row.uri)
		gameCount++
		if (gameCount % 10000 === 0) {
			console.log(`[rebuild] Games: ${gameCount}/${gameRows.length}`)
		}
	}
	console.log(`[rebuild] Games: ${gameCount} entities written`)

	// Rebuild platforms
	console.log('[rebuild] Querying platform records from HappyView...')
	const platformRows = await sql`
		SELECT uri, record->'externalIds'->>'igdb' AS igdb_id
		FROM records
		WHERE collection = 'games.gamesgamesgamesgames.platform'
		  AND record->'externalIds'->>'igdb' IS NOT NULL
	`
	console.log(`[rebuild] Found ${platformRows.length} platform records`)

	let platformCount = 0
	for (const row of platformRows) {
		state.setEntity('platform', row.igdb_id, row.uri)
		platformCount++
	}
	console.log(`[rebuild] Platforms: ${platformCount} entities written`)

	// Summary
	console.log(`\n[rebuild] Done. Games: ${gameCount}, Platforms: ${platformCount}`)
	console.log(`[rebuild] State entity counts: games=${state.getEntityCount('game')}, platforms=${state.getEntityCount('platform')}`)

	await sql.end()
	state.close()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
