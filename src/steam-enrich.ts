/**
 * Steam enrichment — continuous process.
 *
 * Pulls unenriched Steam mappings from state, fetches Steam app
 * details, merges the data into existing atproto game records,
 * and writes them back.
 *
 * Usage:
 *   npx tsx src/steam-enrich.ts
 */

import 'dotenv/config'

import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { SteamClient } from './steam/client.js'
import type { SteamAppDetails } from './steam/client.js'
import { mapSteamGenres, mapSteamCategories, mergeLanguageSupports, buildSystemRequirements } from './steam/mapping.js'
import type { LanguageSupport } from './steam/mapping.js'
import { parseSteamLanguages, parseDescriptionToRichtext } from './steam/parser.js'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GAME_COLLECTION = 'games.gamesgamesgamesgames.game'
const BATCH_SIZE = 50
const SLEEP_MS = 30_000

// ---------------------------------------------------------------------------
// mergeSteamData
// ---------------------------------------------------------------------------

function mergeSteamData(
	existing: Record<string, unknown>,
	steam: SteamAppDetails,
): Record<string, unknown> {
	// Clone to avoid mutating the input
	const record: Record<string, unknown> = { ...existing }

	// 1. Description -> richtext
	if (steam.about_the_game) {
		const { text, facets } = parseDescriptionToRichtext(steam.about_the_game)
		record.description = text
		if (facets.length > 0) {
			record.descriptionFacets = facets
		}
	}

	// 2. System requirements
	const sysReqs = buildSystemRequirements(
		steam.pc_requirements,
		steam.mac_requirements,
		steam.linux_requirements,
	)
	if (sysReqs.length > 0) {
		record.systemRequirements = sysReqs
	}

	// 3. Platform features
	if (steam.categories) {
		const features = mapSteamCategories(steam.categories)
		const steamEntry = { platform: 'steam', features }

		const existingPlatformFeatures = Array.isArray(record.platformFeatures)
			? (record.platformFeatures as Array<{ platform: string; features: string[] }>)
			: []

		// Replace any existing steam entry, keep others
		const filtered = existingPlatformFeatures.filter(
			(pf) => pf.platform !== 'steam',
		)
		filtered.push(steamEntry)
		record.platformFeatures = filtered
	}

	// 4. Genres / themes / modes merge
	if (steam.genres) {
		const { genres, themes, modes } = mapSteamGenres(steam.genres)

		if (genres.length > 0) {
			const existingGenres = Array.isArray(record.genres)
				? (record.genres as string[])
				: []
			const merged = [...new Set([...existingGenres, ...genres])]
			if (merged.length > 0) {
				record.genres = merged
			} else {
				delete record.genres
			}
		}

		if (themes.length > 0) {
			const existingThemes = Array.isArray(record.themes)
				? (record.themes as string[])
				: []
			const merged = [...new Set([...existingThemes, ...themes])]
			if (merged.length > 0) {
				record.themes = merged
			} else {
				delete record.themes
			}
		}

		if (modes.length > 0) {
			const existingModes = Array.isArray(record.modes)
				? (record.modes as string[])
				: []
			const merged = [...new Set([...existingModes, ...modes])]
			if (merged.length > 0) {
				record.modes = merged
			} else {
				delete record.modes
			}
		}
	}

	// 5. Languages cross-reference
	if (steam.supported_languages) {
		const steamLangs = parseSteamLanguages(steam.supported_languages)
		const existingLangs = Array.isArray(record.languageSupports)
			? (record.languageSupports as LanguageSupport[])
			: []
		const merged = mergeLanguageSupports(existingLangs, steamLangs)
		record.languageSupports = merged
	}

	return record
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
	const {
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
	} = process.env

	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		console.error('Missing required environment variables: ATPROTO_SERVICE, ATPROTO_IDENTIFIER, ATPROTO_PASSWORD')
		process.exit(1)
	}

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	const state = new StateManager()
	const steam = new SteamClient()

	let running = true
	const shutdown = () => {
		if (!running) return
		running = false
		console.log('\n[shutdown] Graceful shutdown requested, finishing current batch...')
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	console.log('=== Steam enrichment started ===')

	while (running) {
		const mappings = state.getUnenrichedSteamMappings(BATCH_SIZE)

		if (mappings.length === 0) {
			console.log(`[steam-enrich] No unenriched mappings found, sleeping ${SLEEP_MS / 1000}s...`)
			await new Promise((resolve) => setTimeout(resolve, SLEEP_MS))
			continue
		}

		console.log(`[steam-enrich] Processing batch of ${mappings.length} mappings`)

		for (const mapping of mappings) {
			if (!running) break

			try {
				// Fetch Steam app details
				const steamData = await steam.getAppDetails(Number(mapping.steamId))

				if (!steamData) {
					console.log(`[steam-enrich] Skip IGDB ${mapping.igdbId} — Steam app ${mapping.steamId} not found`)
					state.markSteamEnriched(mapping.igdbId)
					continue
				}

				// Fetch existing atproto record
				const rkey = mapping.atUri.split('/').pop()!
				const existing = await atproto.getRecord(GAME_COLLECTION, rkey)

				if (!existing) {
					console.log(`[steam-enrich] Skip IGDB ${mapping.igdbId} — atproto record not found (rkey ${rkey})`)
					state.markSteamEnriched(mapping.igdbId)
					continue
				}

				// Merge Steam data into existing record
				const { $type: _$type, ...record } = existing
				const updated = mergeSteamData(record, steamData)

				// Write back
				await atproto.putRecord(GAME_COLLECTION, rkey, updated)
				state.markSteamEnriched(mapping.igdbId)

				console.log(`[steam-enrich] Enriched IGDB ${mapping.igdbId} (Steam ${mapping.steamId}) — ${steamData.name}`)
			} catch (err) {
				console.error(`[steam-enrich] Error enriching IGDB ${mapping.igdbId} (Steam ${mapping.steamId}):`, (err as Error).message)
				// Do NOT mark as enriched — will retry next cycle
			}
		}
	}

	console.log('=== Steam enrichment stopped ===')
	state.close()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
