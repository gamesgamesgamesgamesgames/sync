/**
 * Transform IGDB API data → atproto lexicon record shapes.
 */

import type { StateManager } from '../state.js'
import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import { buildMediaItem, buildMediaItemsConcurrent } from '../media.js'
import type { ImageTask } from '../media.js'
import {
	IGDB_PLATFORM_CATEGORY,
	IGDB_WEBSITE_CATEGORY,
	IGDB_GENRE_MAP,
	IGDB_THEME_MAP,
	IGDB_MODE_MAP,
	IGDB_PERSPECTIVE_MAP,
	IGDB_GAME_CATEGORY_MAP,
	IGDB_GAME_TYPE_MAP,
	IGDB_REGION_MAP,
	IGDB_DATE_FORMAT_MAP,
	IGDB_AGE_RATING_ORG_MAP,
	IGDB_AGE_RATING_MAP,
} from '../igdb/types.js'

import type {
	IGDBPlatformFamily,
	IGDBPlatform,
	IGDBGameEngine,
	IGDBCollection,
	IGDBFranchise,
	IGDBGame,
	IGDBInvolvedCompany,
	IGDBPlatformVersion,
	IGDBWebsite as IGDBWebsiteType,
	IGDBReleaseDate,
} from '../igdb/types.js'

const COLLECTION_PLATFORM = 'games.gamesgamesgamesgames.platform'

const PLATFORM_FIELDS = [
	'fields name, abbreviation, alternative_name, category, generation,',
	'summary, slug, platform_family,',
	'platform_logo.image_id, platform_logo.width, platform_logo.height,',
	'versions.name, versions.summary, versions.cpu, versions.graphics,',
	'versions.memory, versions.storage, versions.connectivity,',
	'versions.os, versions.output, versions.resolutions,',
	'websites.type, websites.url;',
].join(' ')

/** In-flight platform creation promises to deduplicate concurrent requests. */
const platformCreationInFlight = new Map<number, Promise<string | undefined>>()

/**
 * Ensure a platform exists in state, creating it on-demand if missing.
 * Returns the platform AT-URI, or undefined if creation failed.
 */
export async function ensurePlatform(
	platformId: number,
	igdbClient: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<string | undefined> {
	// Already in state
	const existing = state.getEntity('platform', platformId)
	if (existing) return existing

	// Check for in-flight creation
	const inflight = platformCreationInFlight.get(platformId)
	if (inflight) return inflight

	const promise = (async () => {
		try {
			const items = await igdbClient.query<IGDBPlatform>(
				'platforms',
				`${PLATFORM_FIELDS} where id = ${platformId}; limit 1;`,
			)
			if (items.length === 0) {
				console.warn(`  [!] Platform ${platformId} not found in IGDB`)
				return undefined
			}

			const record = await mapPlatform(items[0]!, igdbClient, atproto, state)
			const { uri } = await atproto.createRecord(COLLECTION_PLATFORM, record)
			state.setEntity('platform', platformId, uri)
			console.log(`  [+] Created missing platform: ${items[0]!.name} (${uri})`)
			return uri
		} catch (err) {
			console.error(`  [!] Failed to create platform ${platformId}:`, (err as Error).message)
			return undefined
		} finally {
			platformCreationInFlight.delete(platformId)
		}
	})()

	platformCreationInFlight.set(platformId, promise)
	return promise
}

/** Normalize \r\n and \r to \n for clean atproto text fields. */
function normalizeNewlines(text: string): string {
	return text.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
}

/** Map an IGDB Platform Family → lexicon platformFamily record. */
export function mapPlatformFamily(igdb: IGDBPlatformFamily): Record<string, unknown> {
	return {
		name: igdb.name,
		createdAt: new Date().toISOString(),
	}
}

/** Map an IGDB Platform → lexicon platform record. */
export async function mapPlatform(
	igdb: IGDBPlatform,
	igdbClient: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<Record<string, unknown>> {
	const record: Record<string, unknown> = {
		name: igdb.name,
		createdAt: new Date().toISOString(),
	}

	if (igdb.abbreviation) record.abbreviation = igdb.abbreviation
	if (igdb.alternative_name) record.alternativeName = igdb.alternative_name
	if (igdb.summary) record.description = igdb.summary
	if (igdb.generation) record.generation = igdb.generation

	// Platform category
	if (igdb.category != null) {
		const cat = IGDB_PLATFORM_CATEGORY[igdb.category]
		if (cat) record.category = cat
	}

	// Platform family reference
	if (igdb.platform_family != null) {
		const familyId = typeof igdb.platform_family === 'object'
			? igdb.platform_family.id
			: igdb.platform_family
		const familyUri = state.getEntity('platformFamily', familyId)
		if (familyUri) record.family = familyUri
	}

	// Platform logo → media
	if (igdb.platform_logo && typeof igdb.platform_logo === 'object' && igdb.platform_logo.image_id) {
		const logo = igdb.platform_logo
		const mediaItem = await buildMediaItem(
			logo.image_id,
			'icon',
			logo.width,
			logo.height,
			igdbClient,
			atproto,
			state,
		)
		if (mediaItem) record.media = [mediaItem]
	}

	// Platform versions
	if (igdb.versions && igdb.versions.length > 0) {
		record.versions = igdb.versions.map((v: IGDBPlatformVersion) => {
			const ver: Record<string, unknown> = { name: v.name }
			if (v.summary) ver.summary = v.summary
			if (v.cpu) ver.cpu = v.cpu
			if (v.graphics) ver.gpu = v.graphics
			if (v.memory) ver.memory = v.memory
			if (v.storage) ver.storage = v.storage
			if (v.connectivity) ver.connectivity = v.connectivity
			if (v.os) ver.os = v.os
			if (v.output) ver.output = v.output
			if (v.resolutions) ver.maxResolution = v.resolutions
			return ver
		})
	}

	// Websites
	if (igdb.websites && igdb.websites.length > 0) {
		record.websites = mapWebsites(igdb.websites)
	}

	return record
}

/** Map an IGDB Game Engine → lexicon engine record. */
export async function mapEngine(
	igdb: IGDBGameEngine,
	igdbClient: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<Record<string, unknown>> {
	const record: Record<string, unknown> = {
		name: igdb.name,
		createdAt: new Date().toISOString(),
	}

	if (igdb.description) record.description = igdb.description

	// Logo → media
	if (igdb.logo && typeof igdb.logo === 'object' && igdb.logo.image_id) {
		const mediaItem = await buildMediaItem(
			igdb.logo.image_id,
			'icon',
			igdb.logo.width,
			igdb.logo.height,
			igdbClient,
			atproto,
			state,
		)
		if (mediaItem) record.media = [mediaItem]
	}

	// Platform references
	if (igdb.platforms && igdb.platforms.length > 0) {
		const platformUris = igdb.platforms
			.map((id) => state.getEntity('platform', id))
			.filter(Boolean)
		if (platformUris.length > 0) record.platforms = platformUris
	}

	// Websites
	if (igdb.url) {
		record.websites = [{ url: igdb.url, type: 'official' }]
	}

	return record
}

/** Map an IGDB Collection → lexicon collection record. */
export function mapCollection(igdb: IGDBCollection, state?: StateManager): Record<string, unknown> {
	const record: Record<string, unknown> = {
		name: igdb.name,
		type: 'series',
		createdAt: new Date().toISOString(),
	}

	if (state && igdb.games && igdb.games.length > 0) {
		const gameUris = igdb.games
			.map((id) => state.getEntity('game', id))
			.filter(Boolean)
		if (gameUris.length > 0) record.games = gameUris
	}

	return record
}

/** Map an IGDB Franchise → lexicon collection record (with type "franchise"). */
export function mapFranchise(igdb: IGDBFranchise, state?: StateManager): Record<string, unknown> {
	const record: Record<string, unknown> = {
		name: igdb.name,
		type: 'franchise',
		createdAt: new Date().toISOString(),
	}

	if (state && igdb.games && igdb.games.length > 0) {
		const gameUris = igdb.games
			.map((id) => state.getEntity('game', id))
			.filter(Boolean)
		if (gameUris.length > 0) record.games = gameUris
	}

	return record
}

/** Options for mapGame when syncing existing records. */
export interface MapGameOptions {
	/** Existing media array from the matched record. If provided and image IDs
	 *  haven't changed, the existing media is reused without downloading. */
	existingMedia?: Array<Record<string, unknown>>
}

/** Extract external IDs from IGDB game websites. */
function extractExternalIds(igdb: IGDBGame): Record<string, unknown> {
	const ids: Record<string, unknown> = {
		igdb: String(igdb.id),
	}

	for (const website of igdb.websites ?? []) {
		const wtype = website.type ?? website.category
		if (!website.url || wtype == null) continue

		try {
			const url = new URL(website.url)

			switch (wtype) {
				case 13: { // Steam
					const match = url.pathname.match(/\/app\/(\d+)/)
					if (match) ids.steam = match[1]
					break
				}
				case 17: { // GOG
					const match = url.pathname.match(/\/game\/([^/]+)/)
					if (match) ids.gog = match[1]
					break
				}
				case 16: { // Epic
					const match = url.pathname.match(/\/p\/([^/]+)/)
					if (match) ids.epicGames = match[1]
					break
				}
				case 15: { // itch.io
					const dev = url.hostname.replace('.itch.io', '')
					const game = url.pathname.slice(1).split('/')[0]
					if (dev && game) ids.itchIo = { developer: dev, game }
					break
				}
				case 10: // iPhone
				case 11: { // iPad → Apple App Store
					const match = url.pathname.match(/id(\d+)/)
					if (match) ids.appleAppStore = match[1]
					break
				}
				case 12: { // Android → Google Play
					const pkg = url.searchParams.get('id')
					if (pkg) ids.googlePlay = pkg
					break
				}
				case 22: { // Xbox Store
					const match = url.pathname.match(/\/games\/store\/[^/]+\/([a-zA-Z0-9]+)/)
					if (match) ids.xbox = match[1]
					break
				}
				case 23: { // PlayStation Store
					const match = url.pathname.match(/\/concept\/(\d+)/) ?? url.pathname.match(/\/product\/([A-Z0-9_-]+)/)
					if (match) ids.playStation = match[1]
					break
				}
				case 24: { // Nintendo eShop
					const match = url.pathname.match(/\/(\d+)$/) ?? url.pathname.match(/\/games\/detail\/([^/]+)/)
					if (match) ids.nintendoEshop = match[1]
					break
				}
			}
		} catch {
			// Invalid URL — skip
		}
	}

	return ids
}

/** Map an IGDB Game → lexicon game record. */
export async function mapGame(
	igdb: IGDBGame,
	igdbClient: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
	options?: MapGameOptions,
): Promise<Record<string, unknown>> {
	const record: Record<string, unknown> = {
		name: igdb.name,
		externalIds: extractExternalIds(igdb),
		createdAt: new Date().toISOString(),
	}

	if (igdb.summary) record.summary = normalizeNewlines(igdb.summary)
	if (igdb.storyline) record.storyline = normalizeNewlines(igdb.storyline)

	// Application type — prefer game_type (new sub-resource) over deprecated category enum
	const gameTypeStr = typeof igdb.game_type === 'object' ? igdb.game_type?.type : undefined
	const appType = (gameTypeStr && IGDB_GAME_TYPE_MAP[gameTypeStr])
		?? IGDB_GAME_CATEGORY_MAP[igdb.category ?? 0]
	if (appType) record.applicationType = appType

	// Genres
	if (igdb.genres && igdb.genres.length > 0) {
		record.genres = igdb.genres
			.map((g) => IGDB_GENRE_MAP[g.name] ?? g.name.toLowerCase().replace(/[^a-z]/g, ''))
			.filter(Boolean)
	}

	// Themes
	if (igdb.themes && igdb.themes.length > 0) {
		record.themes = igdb.themes
			.map((t) => IGDB_THEME_MAP[t.name] ?? t.name.toLowerCase().replace(/[^a-z]/g, ''))
			.filter(Boolean)
	}

	// Game modes
	if (igdb.game_modes && igdb.game_modes.length > 0) {
		record.modes = igdb.game_modes
			.map((m) => IGDB_MODE_MAP[m.name])
			.filter(Boolean)
	}

	// Player perspectives
	if (igdb.player_perspectives && igdb.player_perspectives.length > 0) {
		record.playerPerspectives = igdb.player_perspectives
			.map((p) => IGDB_PERSPECTIVE_MAP[p.name])
			.filter(Boolean)
	}

	// Parent game
	if (igdb.parent_game) {
		const parentUri = state.getEntity('game', igdb.parent_game)
		if (parentUri) record.parent = parentUri
	}

	// Game engines
	if (igdb.game_engines && igdb.game_engines.length > 0) {
		const engineUris = igdb.game_engines
			.map((id) => state.getEntity('engine', id))
			.filter(Boolean)
		if (engineUris.length > 0) record.engines = engineUris
	}

	// Media (cover, screenshots, artworks) — collected and processed concurrently
	const igdbImageIds = new Set<string>()
	const imageTasks: ImageTask[] = []

	if (igdb.cover && typeof igdb.cover === 'object' && igdb.cover.image_id) {
		igdbImageIds.add(igdb.cover.image_id)
		imageTasks.push({
			imageId: igdb.cover.image_id,
			mediaType: 'cover',
			width: igdb.cover.width,
			height: igdb.cover.height,
		})
	}

	if (igdb.screenshots) {
		for (const ss of igdb.screenshots) {
			if (!ss.image_id) continue
			igdbImageIds.add(ss.image_id)
			imageTasks.push({
				imageId: ss.image_id,
				mediaType: 'screenshot',
				width: ss.width,
				height: ss.height,
			})
		}
	}

	if (igdb.artworks) {
		for (const aw of igdb.artworks) {
			if (!aw.image_id) continue
			igdbImageIds.add(aw.image_id)
			imageTasks.push({
				imageId: aw.image_id,
				mediaType: 'artwork',
				width: aw.width,
				height: aw.height,
			})
		}
	}

	// If existing media is provided, check if image IDs match — reuse if unchanged
	if (options?.existingMedia && options.existingMedia.length > 0) {
		const existingImageIds = new Set(
			options.existingMedia
				.map((m) => m.igdbImageId as string | undefined)
				.filter(Boolean),
		)

		const unchanged = igdbImageIds.size === existingImageIds.size
			&& [...igdbImageIds].every((id) => existingImageIds.has(id))

		if (unchanged) {
			record.media = options.existingMedia
		} else if (imageTasks.length > 0) {
			const media = await buildMediaItemsConcurrent(imageTasks, igdbClient, atproto, state)
			if (media.length > 0) record.media = media
		}
	} else if (imageTasks.length > 0) {
		const media = await buildMediaItemsConcurrent(imageTasks, igdbClient, atproto, state)
		if (media.length > 0) record.media = media
	}

	// Releases (group IGDB release_dates by platform)
	if (igdb.release_dates && igdb.release_dates.length > 0) {
		record.releases = await mapReleaseDates(igdb.release_dates, igdbClient, atproto, state)
	}

	// Age ratings
	if (igdb.age_ratings && igdb.age_ratings.length > 0) {
		record.ageRatings = igdb.age_ratings.map((ar) => {
			const entry: Record<string, unknown> = {}

			// Organization
			const orgId = typeof ar.organization === 'object' ? ar.organization?.id : ar.organization
			if (orgId != null) {
				entry.organization = IGDB_AGE_RATING_ORG_MAP[orgId] ?? String(orgId)
			}

			// Rating
			const ratingId = ar.rating_category ?? ar.rating
			if (ratingId != null) {
				entry.rating = IGDB_AGE_RATING_MAP[ratingId] ?? String(ratingId)
			}

			// Content descriptors
			if (ar.rating_content_descriptions && ar.rating_content_descriptions.length > 0) {
				entry.contentDescriptors = ar.rating_content_descriptions
					.map((d) => d.description)
					.filter(Boolean)
			}

			return entry
		}).filter((ar) => ar.organization && ar.rating)
	}

	// Alternative names
	if (igdb.alternative_names && igdb.alternative_names.length > 0) {
		record.alternativeNames = igdb.alternative_names.map((an) => {
			const entry: Record<string, unknown> = { name: an.name }
			if (an.comment) entry.comment = an.comment
			return entry
		})
	}

	// Keywords
	if (igdb.keywords && igdb.keywords.length > 0) {
		record.keywords = igdb.keywords.map((k) => k.name)
	}

	// Videos (YouTube)
	if (igdb.videos && igdb.videos.length > 0) {
		record.videos = igdb.videos.map((v) => ({
			videoId: v.video_id,
			platform: 'youtube',
			...(v.name ? { title: v.name } : {}),
		}))
	}

	// Websites
	if (igdb.websites && igdb.websites.length > 0) {
		record.websites = mapWebsites(igdb.websites)
	}

	// Language supports
	if (igdb.language_supports && igdb.language_supports.length > 0) {
		const langMap = new Map<string, { audio: boolean; subtitles: boolean; interface: boolean }>()

		for (const ls of igdb.language_supports) {
			const lang = typeof ls.language === 'object' ? ls.language?.name : undefined
			if (!lang) continue

			if (!langMap.has(lang)) {
				langMap.set(lang, { audio: false, subtitles: false, interface: false })
			}

			const entry = langMap.get(lang)!
			const supportType = typeof ls.language_support_type === 'object'
				? ls.language_support_type?.name?.toLowerCase()
				: undefined

			if (supportType === 'audio') entry.audio = true
			else if (supportType === 'subtitles') entry.subtitles = true
			else if (supportType === 'interface') entry.interface = true
		}

		record.languageSupports = Array.from(langMap.entries()).map(([language, support]) => ({
			language,
			...support,
		}))
	}

	// Multiplayer modes
	if (igdb.multiplayer_modes && igdb.multiplayer_modes.length > 0) {
		record.multiplayerModes = await Promise.all(igdb.multiplayer_modes.map(async (mm) => {
			const entry: Record<string, unknown> = {}
			if (mm.platform != null) {
				const platformUri = await ensurePlatform(mm.platform, igdbClient, atproto, state)
				if (platformUri) {
					entry.platformURI = platformUri
				}
			}
			if (mm.onlinemax != null) entry.onlineMax = mm.onlinemax
			if (mm.offlinemax != null) entry.offlineMax = mm.offlinemax
			if (mm.onlinecoopmax != null) entry.onlineCoopMax = mm.onlinecoopmax
			if (mm.offlinecoopmax != null) entry.offlineCoopMax = mm.offlinecoopmax
			if (mm.campaigncoop != null) entry.hasCampaignCoop = mm.campaigncoop
			if (mm.dropin != null) entry.hasDropIn = mm.dropin
			if (mm.lancoop != null) entry.hasLanCoop = mm.lancoop
			if (mm.splitscreen != null) entry.hasSplitscreen = mm.splitscreen
			if (mm.splitscreenonline != null) entry.hasSplitscreenOnline = mm.splitscreenonline
			return entry
		}))
	}

	// Published at (use first release date)
	if (igdb.first_release_date) {
		record.publishedAt = new Date(igdb.first_release_date * 1000).toISOString()
	}

	return record
}

/** Map an IGDB Involved Company → lexicon org.credit record. */
export function mapOrgCredit(
	ic: IGDBInvolvedCompany,
	gameUri: string,
	gameCid: string,
): Record<string, unknown> | null {
	const companyId = typeof ic.company === 'object' ? ic.company?.id : ic.company
	const companyName = typeof ic.company === 'object' ? ic.company?.name : undefined

	if (!companyId) return null

	const roles: string[] = []
	if (ic.developer) roles.push('developer')
	if (ic.publisher) roles.push('publisher')
	if (ic.porting) roles.push('porter')
	if (ic.supporting) roles.push('supporter')

	if (roles.length === 0) return null

	return {
		game: {
			uri: gameUri,
			cid: gameCid,
		},
		roles,
		displayName: companyName ?? `Company ${companyId}`,
		createdAt: new Date().toISOString(),
	}
}

/** Map IGDB websites (with category enum) to lexicon website objects. */
function mapWebsites(websites: IGDBWebsiteType[]): Array<Record<string, unknown>> {
	return websites
		.filter((w) => w.url)
		.map((w) => {
			const wcat = w.type ?? w.category
			const type = wcat != null ? IGDB_WEBSITE_CATEGORY[wcat] : 'other'
			return {
				url: w.url!,
				type: type ?? 'other',
			}
		})
}

/** Group IGDB release_dates by platform into lexicon release objects. */
async function mapReleaseDates(
	releaseDates: IGDBReleaseDate[],
	igdbClient: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<Array<Record<string, unknown>>> {
	// Group by platform ID
	const byPlatform = new Map<number | string, IGDBReleaseDate[]>()

	for (const rd of releaseDates) {
		const platformId = typeof rd.platform === 'object' ? rd.platform?.id : rd.platform
		const key = platformId ?? 'unknown'
		if (!byPlatform.has(key)) byPlatform.set(key, [])
		byPlatform.get(key)!.push(rd)
	}

	return Promise.all(Array.from(byPlatform.entries()).map(async ([platformId, dates]) => {
		const release: Record<string, unknown> = {}

		if (typeof platformId === 'number') {
			const platformUri = await ensurePlatform(platformId, igdbClient, atproto, state)
			if (platformUri) {
				release.platformURI = platformUri
			}
		}

		release.releaseDates = dates.map((rd) => {
			const entry: Record<string, unknown> = {}

			// Date value
			if (rd.date) {
				const d = new Date(rd.date * 1000)
				entry.releasedAt = d.toISOString().split('T')[0]!
			} else if (rd.human) {
				entry.releasedAt = rd.human
			}

			// Date format (IGDB field: date_format, fallback to category for compat)
			const dateFormat = rd.date_format ?? rd.category
			if (dateFormat != null) {
				const fmt = IGDB_DATE_FORMAT_MAP[dateFormat]
				if (fmt) entry.releasedAtFormat = fmt
			}

			// Region (IGDB field: release_region, fallback to region for compat)
			const regionId = rd.release_region ?? rd.region
			if (regionId != null) {
				const region = IGDB_REGION_MAP[regionId]
				if (region) entry.region = region
			}

			// Status
			if (rd.status != null) {
				const statusName = typeof rd.status === 'object' ? rd.status.name : undefined
				if (statusName) {
					entry.status = statusName.toLowerCase().replace(/\s+/g, '')
				}
			}

			return entry
		})

		return release
	}))
}
