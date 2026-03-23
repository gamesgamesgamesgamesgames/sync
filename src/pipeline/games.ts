/**
 * Pipeline phase: Games
 *
 * The most complex phase — fetches games with all expanded sub-resources
 * and creates game records with media uploads.
 *
 * Uses concurrent game mapping (ConcurrencyPool) and prefetches
 * the next IGDB page while processing the current batch.
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBGame } from '../igdb/types.js'
import { mapGame } from '../atproto/mapping.js'
import { errorLabel, prefetch } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'
import { ConcurrencyPool } from '../concurrency.js'

const COLLECTION = 'games.gamesgamesgamesgames.game'
const GAME_CONCURRENCY = 20

/** The IGDB fields query for games with all expanded sub-resources. */
const GAME_FIELDS = [
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
	'first_release_date;',
].join(' ')

export async function scrapeGames(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('games')) {
		console.log('[pipeline] Games already done, skipping')
		return
	}

	console.log('[pipeline] Starting games...')
	const offset = state.getOffset('games')

	let totalProcessed = 0
	const gamePool = new ConcurrencyPool(GAME_CONCURRENCY)

	for await (const { items, offset: currentOffset } of prefetch(
		igdb.paginate<IGDBGame>('games', GAME_FIELDS, offset),
	)) {
		const batchStart = Date.now()
		console.log(`  [fetch] Got ${items.length} games at offset ${currentOffset}`)

		// Map all games in this batch concurrently
		const results = await Promise.allSettled(
			items.map((game) =>
				gamePool.run(async (): Promise<PendingRecord | null> => {
					if (state.hasEntity('game', game.id)) {
						totalProcessed++
						return null
					}

					try {
						const record = await mapGame(game, igdb, atproto, state)
						totalProcessed++

						if (totalProcessed % 10 === 0) {
							console.log(`  [+] Games processed: ${totalProcessed} (current: ${game.name})`)
						}

						return {
							id: game.id,
							entityType: 'game',
							record,
							collection: COLLECTION,
							name: `Game: ${game.name}`,
						}
					} catch (err) {
						console.error(`  [!] ${errorLabel(err)} Failed: ${game.name} (${game.id}):`, (err as Error).message)
						state.addFailure('game', game.id)
						return null
					}
				}),
			),
		)

		// Collect successful results into a batch
		const batch: PendingRecord[] = []
		for (const result of results) {
			if (result.status === 'fulfilled' && result.value) {
				batch.push(result.value)
				if (batch.length >= 200) {
					const flushed = batch.splice(0, 200)
					await flushBatch(flushed, atproto, state)
					for (const entry of flushed) {
						const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
						const steamId = externalIds?.steam as string | undefined
						if (steamId) {
							const atUri = state.getEntity(entry.entityType, entry.id)
							if (atUri) {
								state.setSteamMapping(String(entry.id), steamId, atUri)
							}
						}
					}
				}
			}
		}

		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
			const steamId = externalIds?.steam as string | undefined
			if (steamId) {
				const atUri = state.getEntity(entry.entityType, entry.id)
				if (atUri) {
					state.setSteamMapping(String(entry.id), steamId, atUri)
				}
			}
		}
		state.setOffset('games', currentOffset + items.length)
		state.save()
		const batchElapsed = ((Date.now() - batchStart) / 1000).toFixed(1)
		console.log(`  [offset] Games offset: ${currentOffset + items.length} (batch took ${batchElapsed}s)`)
	}

	// Retry failed games
	const gameFailures = state.getFailures('game')
	if (gameFailures.length > 0) {
		console.log(`[pipeline] Retrying ${gameFailures.length} failed games...`)
		const ids = gameFailures.join(',')
		const retryItems = await igdb.query<IGDBGame>(
			'games',
			`${GAME_FIELDS} where id = (${ids}); limit 500;`,
		)
		const batch: PendingRecord[] = []
		for (const game of retryItems) {
			if (state.hasEntity('game', game.id)) {
				state.removeFailure('game', game.id)
				continue
			}
			try {
				const record = await mapGame(game, igdb, atproto, state)
				batch.push({ id: game.id, entityType: 'game', record, collection: COLLECTION, name: `Retry succeeded: ${game.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					for (const entry of batch) {
						if (state.hasEntity('game', entry.id)) state.removeFailure('game', entry.id)
						const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
						const steamId = externalIds?.steam as string | undefined
						if (steamId) {
							const atUri = state.getEntity(entry.entityType, entry.id)
							if (atUri) {
								state.setSteamMapping(String(entry.id), steamId, atUri)
							}
						}
					}
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Retry failed: ${game.name} (${game.id}):`, (err as Error).message)
			}
		}
		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			if (state.hasEntity('game', entry.id)) state.removeFailure('game', entry.id)
			const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
			const steamId = externalIds?.steam as string | undefined
			if (steamId) {
				const atUri = state.getEntity(entry.entityType, entry.id)
				if (atUri) {
					state.setSteamMapping(String(entry.id), steamId, atUri)
				}
			}
		}
		state.save()
	}

	state.markPhaseDone('games')
	console.log(`[pipeline] Games done (${totalProcessed} total)`)
}

/**
 * Sync existing atproto game records with IGDB data.
 *
 * For each IGDB game:
 * - If it exists in state (has an AT-URI): extract rkey, fetch existing record
 *   for media comparison, and update via putRecord
 * - If not in state: create a new record
 */
export async function syncGames(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('syncGames')) {
		console.log('[pipeline] syncGames already done, skipping')
		return
	}

	console.log('[pipeline] Starting syncGames...')

	const offset = state.getOffset('syncGames')
	let totalUpdated = 0
	let totalCreated = 0
	const gamePool = new ConcurrencyPool(GAME_CONCURRENCY)

	for await (const { items, offset: currentOffset } of prefetch(
		igdb.paginate<IGDBGame>('games', GAME_FIELDS, offset),
	)) {
		const batchStart = Date.now()
		console.log(`  [sync] Got ${items.length} games at offset ${currentOffset}`)

		// Map all games concurrently, checking state for existing AT-URIs
		const mappedResults = await Promise.allSettled(
			items.map((game) =>
				gamePool.run(async () => {
					try {
						const existingUri = state.getEntity('game', game.id)
						let existingMedia: Array<Record<string, unknown>> | undefined
						let rkey: string | undefined

						if (existingUri) {
							// Extract rkey from AT-URI: at://did/collection/rkey
							rkey = existingUri.split('/').pop()!
							// Fetch existing record for media comparison
							const existingRecord = await atproto.getRecord(COLLECTION, rkey)
							if (existingRecord) {
								existingMedia = existingRecord.media as Array<Record<string, unknown>> | undefined
							}
						}

						const record = await mapGame(game, igdb, atproto, state, { existingMedia })
						return { game, record, rkey }
					} catch (err) {
						console.error(`  [!] ${errorLabel(err)} Sync map failed: ${game.name} (${game.id}):`, (err as Error).message)
						return null
					}
				}),
			),
		)

		// Process results: collect updates and creates into batches
		const updateBatch: Array<{ igdbId: number; rkey: string; record: Record<string, unknown>; name: string }> = []
		const createBatch: PendingRecord[] = []

		for (const result of mappedResults) {
			if (result.status !== 'fulfilled' || !result.value) continue
			const { game, record, rkey } = result.value

			if (rkey) {
				updateBatch.push({ igdbId: game.id, rkey, record, name: game.name })
			} else {
				createBatch.push({
					id: game.id,
					entityType: 'game',
					record,
					collection: COLLECTION,
					name: `Sync new: ${game.name}`,
				})
			}
		}

		// Flush updates in batches of 10 (game records with media are large, 2MB applyWrites limit)
		for (let i = 0; i < updateBatch.length; i += 10) {
			const chunk = updateBatch.slice(i, i + 10)
			try {
				const results = await atproto.applyUpdates(
					chunk.map((u) => ({ collection: COLLECTION, rkey: u.rkey, record: u.record })),
				)
				for (let j = 0; j < chunk.length; j++) {
					const entry = chunk[j]!
					const result = results[j]
					if (result) {
						state.setEntity('game', entry.igdbId, result.uri)
						const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
						const steamId = externalIds?.steam as string | undefined
						if (steamId) {
							state.setSteamMapping(String(entry.igdbId), steamId, result.uri)
						}
						console.log(`  [sync] Updated: ${entry.name} (${result.uri})`)
					}
				}
				totalUpdated += chunk.length
			} catch (err) {
				// Batch rejected — fall back to individual putRecord
				console.warn(`  [!] Update batch of ${chunk.length} rejected (${(err as Error).message}), falling back to individual puts`)
				for (const entry of chunk) {
					try {
						const { uri } = await atproto.putRecord(COLLECTION, entry.rkey, entry.record)
						state.setEntity('game', entry.igdbId, uri)
						const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
						const steamId = externalIds?.steam as string | undefined
						if (steamId) {
							state.setSteamMapping(String(entry.igdbId), steamId, uri)
						}
						totalUpdated++
						console.log(`  [sync] Updated: ${entry.name} (${uri})`)
					} catch (putErr) {
						console.error(`  [!] ${errorLabel(putErr)} Sync update failed: ${entry.name} (${entry.igdbId}):`, (putErr as Error).message)
					}
				}
			}
		}

		// Flush creates in batches of 200
		for (let i = 0; i < createBatch.length; i += 200) {
			const chunk = createBatch.slice(i, i + 200)
			await flushBatch(chunk, atproto, state)
			for (const entry of chunk) {
				const externalIds = entry.record.externalIds as Record<string, unknown> | undefined
				const steamId = externalIds?.steam as string | undefined
				if (steamId) {
					const atUri = state.getEntity(entry.entityType, entry.id)
					if (atUri) {
						state.setSteamMapping(String(entry.id), steamId, atUri)
					}
				}
			}
			totalCreated += chunk.length
		}

		state.setOffset('syncGames', currentOffset + items.length)
		state.save()
		const batchElapsed = ((Date.now() - batchStart) / 1000).toFixed(1)
		console.log(`  [sync] Offset: ${currentOffset + items.length} (batch took ${batchElapsed}s)`)
	}

	state.markPhaseDone('syncGames')
	console.log(`[pipeline] syncGames done (updated=${totalUpdated}, created=${totalCreated})`)
}
