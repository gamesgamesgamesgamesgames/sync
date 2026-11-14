/**
 * Nightly IGDB → atproto sync
 *
 * Incrementally syncs changes from IGDB to the PDS using
 * updated_at timestamps to fetch only modified entities.
 *
 * Usage:
 *   npx tsx src/sync.ts
 *
 * Cron:
 *   0 4 * * * cd /root/igdb-scrape && npx tsx src/sync.ts >> sync.log 2>&1
 */

import 'dotenv/config'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import {
	// mapPlatformFamily,
	mapPlatform,
	// mapEngine,
	mapCollection,
	mapFranchise,
	mapGame,
} from './atproto/mapping.js'
import { needsAttention, syncEntityType, type SyncEntityConfig, type SyncResult } from './pipeline/sync-entities.js'
import { ContributionClient } from './contributions/client.js'
import { syncNewCompanies } from './pipeline/sync-companies.js'
import { slugify } from './helpers.js'
import { MirroringBlobUploader, assertCatalogueAccount } from './media.js'
import type {
	// IGDBPlatformFamily,
	IGDBPlatform,
	// IGDBGameEngine,
	IGDBCollection,
	IGDBFranchise,
	IGDBGame,
} from './igdb/types.js'

async function main() {
	const {
		TWITCH_CLIENT_ID,
		TWITCH_CLIENT_SECRET,
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
		HAPPYVIEW_URL,
	} = process.env

	if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
		throw new Error('Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET in .env')
	}
	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD in .env')
	}
	if (!HAPPYVIEW_URL) {
		throw new Error('Missing HAPPYVIEW_URL in .env')
	}

	const {
		IGDB_ATPROTO_DID,
		HAPPYVIEW_CLIENT_ID,
		HAPPYVIEW_CLIENT_KEY,
		HAPPYVIEW_CLIENT_SECRET,
	} = process.env

	if (!IGDB_ATPROTO_DID || !HAPPYVIEW_CLIENT_ID || !HAPPYVIEW_CLIENT_KEY || !HAPPYVIEW_CLIENT_SECRET) {
		throw new Error('Missing IGDB_ATPROTO_DID, HAPPYVIEW_CLIENT_ID, HAPPYVIEW_CLIENT_KEY, or HAPPYVIEW_CLIENT_SECRET in .env')
	}

	const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
	await igdb.authenticate()

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	const contributionClient = new ContributionClient({
		instanceUrl: HAPPYVIEW_URL,
		clientId: HAPPYVIEW_CLIENT_ID,
		clientKey: HAPPYVIEW_CLIENT_KEY,
		clientSecret: HAPPYVIEW_CLIENT_SECRET,
	})
	await contributionClient.restore(IGDB_ATPROTO_DID)

	const state = new StateManager()

	// Runs that need a human end with a non-zero exit, so systemd OnFailure
	// can alert on them.
	const attention: string[] = []
	const track = (name: string, result: SyncResult) => {
		if (needsAttention(result)) {
			attention.push(`${name}: ${result.aborted ?? (result.cursorAlert ? 'cursor is stale' : `${result.systemic} systemic failure(s)`)}`)
		}
	}

	// Game media goes to the contributor's repo (for the contribution record)
	// and to the catalogue repo (for when auto-accept applies it).
	assertCatalogueAccount(atproto.getDid(), process.env.PENTARACT_DID)
	const blobUploader = new MirroringBlobUploader(contributionClient, atproto, state)

	const shutdown = () => {
		console.log('\n[shutdown] Closing state...')
		state.close()
		process.exit(0)
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	console.log('=== IGDB → atproto Nightly Sync ===')
	console.log(`Started at ${new Date().toISOString()}`)
	console.log()

	// Sync in dependency order

	// // 1. Platform Families
	// const platformFamilyConfig: SyncEntityConfig<IGDBPlatformFamily> = {
	// 	entityType: 'platformFamily',
	// 	igdbEndpoint: 'platform_families',
	// 	igdbFields: 'fields name, slug, updated_at;',
	// 	collection: 'games.gamesgamesgamesgames.platformFamily',
	// 	mapRecord: async (item) => mapPlatformFamily(item),
	// }
	// await syncEntityType(platformFamilyConfig, igdb, atproto, state)

	// 2. Platforms
	const platformConfig: SyncEntityConfig<IGDBPlatform> = {
		entityType: 'platform',
		igdbEndpoint: 'platforms',
		igdbFields: [
			'fields name, abbreviation, alternative_name, category, generation,',
			'summary, slug, platform_family,',
			'platform_logo.image_id, platform_logo.width, platform_logo.height,',
			'versions.name, versions.summary, versions.cpu, versions.graphics,',
			'versions.memory, versions.storage, versions.connectivity,',
			'versions.os, versions.output, versions.resolutions,',
			'websites.type, websites.url, updated_at;',
		].join(' '),
		collection: 'games.gamesgamesgamesgames.platform',
		mapRecord: async (item) => mapPlatform(item, igdb, atproto, state),
	}
	track('platform', await syncEntityType(platformConfig, igdb, atproto, state))

	// // 3. Engines
	// const engineConfig: SyncEntityConfig<IGDBGameEngine> = {
	// 	entityType: 'engine',
	// 	igdbEndpoint: 'game_engines',
	// 	igdbFields: [
	// 		'fields name, description, slug, url,',
	// 		'logo.image_id, logo.width, logo.height,',
	// 		'platforms, companies, updated_at;',
	// 	].join(' '),
	// 	collection: 'games.gamesgamesgamesgames.engine',
	// 	mapRecord: async (item) => mapEngine(item, igdb, atproto, state),
	// }
	// await syncEntityType(engineConfig, igdb, atproto, state)

	// 4. Games
	const gameConfig: SyncEntityConfig<IGDBGame> = {
		entityType: 'game',
		contributionClient,
		igdbEndpoint: 'games',
		igdbFields: [
			'fields name, summary, storyline, slug, category, game_type.type,',
			'cover.image_id, cover.width, cover.height,',
			'screenshots.image_id, screenshots.width, screenshots.height,',
			'artworks.image_id, artworks.width, artworks.height,',
			'genres.name, genres.slug,',
			'themes.name, themes.slug,',
			'game_modes.name, game_modes.slug,',
			'player_perspectives.name, player_perspectives.slug,',
			'platforms,',
			'game_engines,',
			'release_dates.date, release_dates.human, release_dates.date_format,',
			'release_dates.platform, release_dates.release_region, release_dates.status.name,',
			'age_ratings.organization, age_ratings.rating, age_ratings.rating_category,',
			'age_ratings.rating_content_descriptions.description,',
			'alternative_names.name, alternative_names.comment,',
			'keywords.name,',
			'videos.video_id, videos.name,',
			'websites.type, websites.url,',
			'language_supports.language.name, language_supports.language_support_type.name,',
			'multiplayer_modes.*,',
			'parent_game, collections, franchises,',
			'involved_companies.company.id, involved_companies.company.name,',
			'involved_companies.developer, involved_companies.publisher,',
			'involved_companies.porting, involved_companies.supporting,',
			'first_release_date, updated_at;',
		].join(' '),
		collection: 'games.gamesgamesgamesgames.game',
		updateBatchSize: 10,
		concurrency: 20,
		getSlug: (item) => item.slug || slugify(item.name),
		happyviewUrl: HAPPYVIEW_URL,
		happyviewApiKey: process.env.HAPPYVIEW_API_KEY,
		startOffset: state.getLastSyncAt('game') ? 0 : state.getEntityCount('game'),
		mapRecord: async (item) => {
			// For existing games, fetch the current record for media comparison
			const existingUri = state.getEntity('game', item.id)
			let existingMedia: Array<Record<string, unknown>> | undefined

			if (existingUri) {
				const rkey = existingUri.split('/').pop()!
				const existingRecord = await atproto.getRecord('games.gamesgamesgamesgames.game', rkey)
				if (existingRecord) {
					existingMedia = existingRecord.media as Array<Record<string, unknown>> | undefined
				}
			}

			return mapGame(item, igdb, atproto, state, { existingMedia, blobUploader })
		},
	}
	track('game', await syncEntityType(gameConfig, igdb, atproto, state))

	// 5. Collections (IGDB collections → type: "series") — after games so game URIs are available
	const collectionConfig: SyncEntityConfig<IGDBCollection> = {
		entityType: 'collection',
		igdbEndpoint: 'collections',
		igdbFields: 'fields name, slug, games, url, updated_at;',
		collection: 'games.gamesgamesgamesgames.collection',
		getStateKey: (item) => `c_${item.id}`,
		mapRecord: async (item) => mapCollection(item, state),
	}
	track('collection', await syncEntityType(collectionConfig, igdb, atproto, state))

	// 6. Franchises (IGDB franchises → type: "franchise")
	const franchiseConfig: SyncEntityConfig<IGDBFranchise> = {
		entityType: 'collection',
		igdbEndpoint: 'franchises',
		igdbFields: 'fields name, slug, games, url, updated_at;',
		collection: 'games.gamesgamesgamesgames.collection',
		getStateKey: (item) => `f_${item.id}`,
		mapRecord: async (item) => mapFranchise(item, state),
	}
	track('franchise', await syncEntityType(franchiseConfig, igdb, atproto, state))

	// 7. Sync new companies — create org profiles for any companies
	//    encountered in game updates that don't have profiles yet.
	await syncNewCompanies(igdb, atproto, state)

	// 8. Queue a sweep of our own pending contributions, so media deferrals
	//    and failed auto-accept jobs get retried once a night. Best effort: a
	//    failure here only needs attention, it loses nothing.
	try {
		const jobId = await contributionClient.queueAutoAcceptSweep()
		console.log(`[contributions] Queued auto-accept sweep ${jobId}`)
	} catch (err) {
		console.error('[contributions] Could not queue the auto-accept sweep:', (err as Error).message)
		attention.push(`auto-accept sweep: ${(err as Error).message}`)
	}

	// Done
	console.log()
	console.log('=== Sync complete! ===')
	console.log(`Finished at ${new Date().toISOString()}`)

	state.close()

	if (attention.length > 0) {
		console.error(`[sync] [ALERT] This run needs attention: ${attention.join('; ')}`)
		process.exitCode = 1
	}
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
