/**
 * IGDB → atproto scraper
 *
 * Fetches game data from IGDB and creates atproto records
 * in the @gamesgamesgamesgames.games repo.
 *
 * Supports resume — if interrupted, re-run to pick up where it left off.
 *
 * Usage:
 *   1. Copy .env.example to .env and fill in credentials
 *   2. npm install
 *   3. npm start
 */

import 'dotenv/config'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { scrapePlatformFamilies, scrapePlatforms } from './pipeline/platforms.js'
import { scrapeEngines } from './pipeline/engines.js'
import { scrapeCollections } from './pipeline/collections.js'
import { scrapeGames, syncGames } from './pipeline/games.js'
import { scrapeCredits } from './pipeline/credits.js'

async function main() {
	// Validate environment
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

	// Initialize clients
	const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
	await igdb.authenticate()

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	// Initialize state manager (loads existing state for resume)
	const state = new StateManager()

	// Register graceful shutdown — save state on SIGINT/SIGTERM
	const shutdown = () => {
		console.log('\n[shutdown] Closing state...')
		state.close()
		console.log(`[stats] Records created this session: ${atproto.getTotalRecordCount()}`)
		console.log('[shutdown] Done. Exiting.')
		process.exit(0)
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	console.log('=== IGDB → atproto Scraper ===')
	console.log(`Resuming from phase: ${state.getCurrentPhase()}`)
	console.log()

	// Execute pipeline phases in order
	// Each phase checks if it's already done and skips if so

	// Phase 1: Platform Families
	if (!state.isPhaseDone('platformFamilies')) {
		state.setPhase('platformFamilies')
	}
	await scrapePlatformFamilies(igdb, atproto, state)

	// Phase 2: Platforms
	if (!state.isPhaseDone('platforms')) {
		state.setPhase('platforms')
	}
	await scrapePlatforms(igdb, atproto, state)

	// Phase 3: Engines
	if (!state.isPhaseDone('engines')) {
		state.setPhase('engines')
	}
	await scrapeEngines(igdb, atproto, state)

	// Phase 4: Collections + Franchises
	if (!state.isPhaseDone('collections')) {
		state.setPhase('collections')
	}
	await scrapeCollections(igdb, atproto, state)

	// Phase 5: Games
	if (!state.isPhaseDone('games')) {
		state.setPhase('games')
	}
	await scrapeGames(igdb, atproto, state)

	// Phase 6: Sync Games (update existing records with IGDB data)
	if (!state.isPhaseDone('syncGames')) {
		state.setPhase('syncGames')
	}
	await syncGames(igdb, atproto, state)

	// Phase 7: Org Credits
	if (!state.isPhaseDone('credits')) {
		state.setPhase('credits')
	}
	await scrapeCredits(igdb, atproto, state)

	// Done!
	console.log()
	console.log('=== Scrape complete! ===')
	console.log(`[stats] Records created this session: ${atproto.getTotalRecordCount()}`)

	const entityTypes: Array<import('./state.js').EntityType> = ['platformFamily', 'platform', 'engine', 'collection', 'game', 'orgCredit']
	const totalEntities = entityTypes.reduce((sum, type) => sum + state.getEntityCount(type), 0)
	const totalFailures = state.getFailureCount()

	console.log(`Total records created: ${totalEntities}`)
	console.log(`Total failures: ${totalFailures}`)

	if (totalFailures > 0) {
		console.log('Failed IDs by type:')
		for (const type of entityTypes) {
			const count = state.getFailureCount(type)
			if (count > 0) {
				console.log(`  ${type}: ${count} failures`)
			}
		}
	}

	state.close()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
