import type { SteamLanguage, SystemSpec } from './parser.js'
import { parseSystemRequirements } from './parser.js'
import type { SteamRequirements } from './client.js'

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

export interface LanguageSupport {
	language: string
	audio?: boolean
	subtitles?: boolean
	interface?: boolean
}

export interface SystemRequirements {
	platform: string
	minimum?: SystemSpec
	recommended?: SystemSpec
}

export interface SteamGenreResult {
	genres: string[]
	themes: string[]
	modes: string[]
}

// ---------------------------------------------------------------------------
// Genre mapping
// ---------------------------------------------------------------------------

const GENRE_TO_GENRES: Record<string, string> = {
	Adventure: 'adventure',
	Casual: 'casual',
	Indie: 'indie',
	Racing: 'racing',
	RPG: 'rpg',
	Simulation: 'simulator',
	Sports: 'sport',
	Strategy: 'strategy',
}

const GENRE_TO_THEMES: Record<string, string> = {
	Action: 'action',
}

const GENRE_TO_MODES: Record<string, string> = {
	'Massively Multiplayer': 'mmo',
}

const SKIP_GENRES = new Set([
	'Free to Play',
	'Early Access',
	'Utilities',
	'Design & Illustration',
	'Audio Production',
	'Video Production',
	'Web Publishing',
	'Education',
	'Software Training',
	'Accounting',
	'Photo Editing',
	'Animation & Modeling',
	'Game Development',
])

// ---------------------------------------------------------------------------
// Category mapping
// ---------------------------------------------------------------------------

const CATEGORY_MAP: Record<number, string> = {
	1: 'multiPlayer',
	2: 'singlePlayer',
	9: 'coop',
	18: 'controllerSupport',
	22: 'achievements',
	23: 'cloudSaves',
	25: 'leaderboards',
	27: 'crossPlatformMultiplayer',
	28: 'controllerSupportFull',
	29: 'steamTradingCards',
	30: 'steamWorkshop',
	35: 'inAppPurchases',
	36: 'multiPlayerOnline',
	37: 'multiPlayerLan',
	38: 'coopOnline',
	39: 'coopLan',
	41: 'remotePlayTogether',
	43: 'remotePlayTV',
	44: 'remotePlayPhone',
	45: 'remotePlayTablet',
	46: 'pvpOnline',
	47: 'pvpLan',
	51: 'familySharing',
	52: 'vrSupport',
	61: 'trackingAndManagement',
	62: 'moddingSupport',
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Convert a string to camelCase.
 * "Massively Multiplayer" -> "massivelyMultiplayer"
 * "Role Playing" -> "rolePlaying"
 */
function toCamelCase(str: string): string {
	return str
		.split(/[\s_-]+/)
		.map((word, i) => {
			const lower = word.toLowerCase()
			if (i === 0) return lower
			return lower.charAt(0).toUpperCase() + lower.slice(1)
		})
		.join('')
}

// ---------------------------------------------------------------------------
// 1. mapSteamGenres
// ---------------------------------------------------------------------------

export function mapSteamGenres(
	steamGenres: Array<{ id: string; description: string }>,
): SteamGenreResult {
	const genres: string[] = []
	const themes: string[] = []
	const modes: string[] = []

	for (const genre of steamGenres) {
		const name = genre.description

		if (SKIP_GENRES.has(name)) continue

		if (GENRE_TO_GENRES[name] !== undefined) {
			genres.push(GENRE_TO_GENRES[name]!)
		} else if (GENRE_TO_THEMES[name] !== undefined) {
			themes.push(GENRE_TO_THEMES[name]!)
		} else if (GENRE_TO_MODES[name] !== undefined) {
			modes.push(GENRE_TO_MODES[name]!)
		} else {
			// Unknown genre: convert to camelCase and add to genres
			genres.push(toCamelCase(name))
		}
	}

	return { genres, themes, modes }
}

// ---------------------------------------------------------------------------
// 2. mapSteamCategories
// ---------------------------------------------------------------------------

export function mapSteamCategories(
	categories: Array<{ id: number; description: string }>,
): string[] {
	const features = new Set<string>()

	for (const cat of categories) {
		const feature = CATEGORY_MAP[cat.id]
		if (feature) {
			features.add(feature)
		}
	}

	return [...features]
}

// ---------------------------------------------------------------------------
// 3. mergeLanguageSupports
// ---------------------------------------------------------------------------

export function mergeLanguageSupports(
	igdbLangs: LanguageSupport[],
	steamLangs: SteamLanguage[],
): LanguageSupport[] {
	const map = new Map<string, LanguageSupport>()

	// Add all IGDB entries first (they take precedence)
	for (const entry of igdbLangs) {
		map.set(entry.language, { ...entry })
	}

	// Add Steam-only languages with defaults
	for (const entry of steamLangs) {
		if (!map.has(entry.language)) {
			map.set(entry.language, {
				language: entry.language,
				audio: entry.audio,
				subtitles: true,
				interface: true,
			})
		}
	}

	return [...map.values()]
}

// ---------------------------------------------------------------------------
// 4. buildSystemRequirements
// ---------------------------------------------------------------------------

function isEmptySpec(spec: SystemSpec): boolean {
	return Object.keys(spec).length === 0
}

export function buildSystemRequirements(
	pcReqs: SteamRequirements | [],
	macReqs: SteamRequirements | [],
	linuxReqs: SteamRequirements | [],
): SystemRequirements[] {
	const platforms: Array<{
		name: string
		reqs: SteamRequirements | []
	}> = [
		{ name: 'windows', reqs: pcReqs },
		{ name: 'mac', reqs: macReqs },
		{ name: 'linux', reqs: linuxReqs },
	]

	const result: SystemRequirements[] = []

	for (const { name, reqs } of platforms) {
		// Skip if input is an empty array
		if (Array.isArray(reqs)) continue

		const minimum = parseSystemRequirements(reqs.minimum)
		const recommended = parseSystemRequirements(reqs.recommended)

		// Skip if both specs are empty
		if (isEmptySpec(minimum) && isEmptySpec(recommended)) continue

		const entry: SystemRequirements = { platform: name }
		if (!isEmptySpec(minimum)) entry.minimum = minimum
		if (!isEmptySpec(recommended)) entry.recommended = recommended

		result.push(entry)
	}

	return result
}
