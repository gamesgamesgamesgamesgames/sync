/** IGDB API response types */

export interface IGDBPlatformFamily {
	id: number
	name: string
	slug?: string
	updated_at?: number
}

export interface IGDBPlatformLogo {
	id: number
	image_id: string
	width?: number
	height?: number
	url?: string
}

export interface IGDBPlatformVersion {
	id: number
	name: string
	summary?: string
	cpu?: string
	graphics?: string
	memory?: string
	storage?: string
	connectivity?: string
	os?: string
	output?: string
	resolutions?: string
	sound?: string
	media?: string
	main_manufacturer?: number
	slug?: string
}

export interface IGDBPlatformWebsite {
	id: number
	category?: number
	type?: number
	url?: string
}

export interface IGDBPlatform {
	id: number
	name: string
	abbreviation?: string
	alternative_name?: string
	category?: number
	generation?: number
	summary?: string
	slug?: string
	platform_family?: IGDBPlatformFamily | number
	platform_logo?: IGDBPlatformLogo | number
	versions?: IGDBPlatformVersion[]
	websites?: IGDBPlatformWebsite[]
	updated_at?: number
}

export interface IGDBGameEngine {
	id: number
	name: string
	description?: string
	slug?: string
	logo?: { id: number; image_id: string; width?: number; height?: number } | number
	platforms?: number[]
	companies?: number[]
	url?: string
	updated_at?: number
}

export interface IGDBCollection {
	id: number
	name: string
	slug?: string
	games?: number[]
	url?: string
	updated_at?: number
}

export interface IGDBFranchise {
	id: number
	name: string
	slug?: string
	games?: number[]
	url?: string
	updated_at?: number
}

export interface IGDBCover {
	id: number
	image_id: string
	width?: number
	height?: number
	game?: number
}

export interface IGDBScreenshot {
	id: number
	image_id: string
	width?: number
	height?: number
	game?: number
}

export interface IGDBArtwork {
	id: number
	image_id: string
	width?: number
	height?: number
	game?: number
}

export interface IGDBGenre {
	id: number
	name: string
	slug?: string
}

export interface IGDBTheme {
	id: number
	name: string
	slug?: string
}

export interface IGDBGameMode {
	id: number
	name: string
	slug?: string
}

export interface IGDBPlayerPerspective {
	id: number
	name: string
	slug?: string
}

export interface IGDBReleaseDate {
	id: number
	date?: number
	human?: string
	category?: number
	date_format?: number
	platform?: IGDBPlatform | number
	region?: number
	release_region?: number
	status?: { id: number; name?: string } | number
	game?: number
	y?: number
	m?: number
}

export interface IGDBAgeRating {
	id: number
	organization?: { id: number; name?: string } | number
	rating_category?: number
	rating?: number
	rating_content_descriptions?: Array<{ id: number; description?: string }>
	synopsis?: string
}

export interface IGDBAlternativeName {
	id: number
	name: string
	comment?: string
	game?: number
}

export interface IGDBKeyword {
	id: number
	name: string
	slug?: string
}

export interface IGDBGameVideo {
	id: number
	video_id: string
	name?: string
	game?: number
}

export interface IGDBWebsite {
	id: number
	category?: number
	type?: number
	url?: string
	game?: number
}

export interface IGDBLanguageSupport {
	id: number
	language?: { id: number; name?: string; native_name?: string; locale?: string } | number
	language_support_type?: { id: number; name?: string } | number
	game?: number
}

export interface IGDBMultiplayerMode {
	id: number
	campaigncoop?: boolean
	dropin?: boolean
	lancoop?: boolean
	offlinecoop?: boolean
	offlinecoopmax?: number
	offlinemax?: number
	onlinecoop?: boolean
	onlinecoopmax?: number
	onlinemax?: number
	platform?: number
	splitscreen?: boolean
	splitscreenonline?: boolean
	game?: number
}

export interface IGDBCompany {
	id: number
	name: string
	slug?: string
	description?: string
	country?: number
	start_date?: number
	start_date_category?: number
	logo?: { id: number; image_id: string; width?: number; height?: number } | number
	websites?: IGDBWebsite[]
	parent?: { id: number; name?: string } | number
	status?: { id: number; name?: string } | number
	developed?: number[]
	published?: number[]
	updated_at?: number
}

export interface IGDBInvolvedCompany {
	id: number
	company?: { id: number; name?: string; slug?: string } | number
	game?: number
	developer?: boolean
	publisher?: boolean
	porting?: boolean
	supporting?: boolean
	updated_at?: number
}

export interface IGDBGame {
	id: number
	name: string
	summary?: string
	storyline?: string
	slug?: string
	category?: number
	game_type?: { id: number; type?: string } | number
	cover?: IGDBCover | number
	screenshots?: IGDBScreenshot[]
	artworks?: IGDBArtwork[]
	genres?: IGDBGenre[]
	themes?: IGDBTheme[]
	game_modes?: IGDBGameMode[]
	player_perspectives?: IGDBPlayerPerspective[]
	platforms?: number[]
	game_engines?: number[]
	release_dates?: IGDBReleaseDate[]
	age_ratings?: IGDBAgeRating[]
	alternative_names?: IGDBAlternativeName[]
	keywords?: IGDBKeyword[]
	videos?: IGDBGameVideo[]
	websites?: IGDBWebsite[]
	language_supports?: IGDBLanguageSupport[]
	multiplayer_modes?: IGDBMultiplayerMode[]
	parent_game?: number
	collections?: number[]
	franchises?: number[]
	involved_companies?: IGDBInvolvedCompany[]
	first_release_date?: number
	created_at?: number
	updated_at?: number
}

/** IGDB platform category enum → lexicon platformCategory mapping */
export const IGDB_PLATFORM_CATEGORY: Record<number, string> = {
	1: 'console',
	2: 'arcade',
	3: 'platform', // "platform" in IGDB means generic platform
	4: 'operatingSystem',
	5: 'portable',
	6: 'computer',
}

/** IGDB website category enum → lexicon website type mapping */
export const IGDB_WEBSITE_CATEGORY: Record<number, string> = {
	1: 'official',
	2: 'wiki',
	3: 'wikipedia',
	4: 'facebook',
	5: 'twitter',
	6: 'twitch',
	8: 'instagram',
	9: 'youtube',
	10: 'other', // iPhone
	11: 'other', // iPad
	12: 'other', // Android
	13: 'steam',
	14: 'reddit',
	15: 'itchIo',
	16: 'epicGames',
	17: 'gog',
	18: 'discord',
	19: 'bluesky',
	22: 'xbox',
	23: 'playstation',
	24: 'nintendo',
	25: 'meta',
}

/** IGDB genre name → lexicon genre knownValues mapping */
export const IGDB_GENRE_MAP: Record<string, string> = {
	'Fighting': 'fighting',
	'Music': 'music',
	'Platform': 'platform',
	'Point-and-click': 'pointAndClick',
	'Puzzle': 'puzzle',
	'Racing': 'racing',
	'Role-playing (RPG)': 'rpg',
	'Real Time Strategy (RTS)': 'rts',
	'Shooter': 'shooter',
	'Simulator': 'simulator',
	// Genres in IGDB that don't have a direct knownValue — pass them through as-is (lowercased)
	'Adventure': 'adventure',
	'Arcade': 'arcade',
	'Card & Board Game': 'cardAndBoardGame',
	'Hack and slash/Beat \'em up': 'hackAndSlash',
	'Indie': 'indie',
	'MOBA': 'moba',
	'Pinball': 'pinball',
	'Quiz/Trivia': 'quizTrivia',
	'Sport': 'sport',
	'Strategy': 'strategy',
	'Tactical': 'tactical',
	'Turn-based strategy (TBS)': 'tbs',
	'Visual Novel': 'visualNovel',
}

/** IGDB theme name → lexicon theme knownValues mapping */
export const IGDB_THEME_MAP: Record<string, string> = {
	'4X (explore, expand, exploit, and exterminate)': '4x',
	'Action': 'action',
	'Business': 'business',
	'Comedy': 'comedy',
	'Drama': 'drama',
	'Educational': 'educational',
	'Erotic': 'erotic',
	'Fantasy': 'fantasy',
	'Historical': 'historical',
	'Horror': 'horror',
	'Kids': 'kids',
	'Mystery': 'mystery',
	'Non-fiction': 'nonfiction',
	'Open world': 'openWorld',
	'Party': 'party',
	'Romance': 'romance',
	'Sandbox': 'sandbox',
	'Science fiction': 'scifi',
	'Stealth': 'stealth',
	'Survival': 'survival',
	'Thriller': 'thriller',
	'Warfare': 'warfare',
}

/** IGDB game mode name → lexicon mode knownValues mapping */
export const IGDB_MODE_MAP: Record<string, string> = {
	'Battle Royale': 'battleRoyale',
	'Co-operative': 'cooperative',
	'Massively Multiplayer Online (MMO)': 'mmo',
	'Multiplayer': 'multiplayer',
	'Single player': 'singlePlayer',
	'Split screen': 'splitScreen',
}

/** IGDB player perspective name → lexicon playerPerspective mapping */
export const IGDB_PERSPECTIVE_MAP: Record<string, string> = {
	'Auditory': 'auditory',
	'Bird view / Isometric': 'isometric',
	'First person': 'firstPerson',
	'Side view': 'sideView',
	'Text': 'text',
	'Third person': 'thirdPerson',
	'Virtual Reality': 'vr',
}

/** @deprecated IGDB category enum → lexicon applicationType mapping (category field is deprecated) */
export const IGDB_GAME_CATEGORY_MAP: Record<number, string> = {
	0: 'game',
	1: 'dlc',
	2: 'expansion',
	3: 'bundle',
	4: 'standaloneExpansion',
	5: 'mod',
	6: 'episode',
	7: 'season',
	8: 'remake',
	9: 'remaster',
	10: 'expandedGame',
	11: 'port',
	12: 'fork',
	13: 'addon',
	14: 'update',
}

/** IGDB game_type.type string → lexicon applicationType mapping */
export const IGDB_GAME_TYPE_MAP: Record<string, string> = {
	'Main Game': 'game',
	'DLC': 'dlc',
	'Expansion': 'expansion',
	'Bundle': 'bundle',
	'Standalone Expansion': 'standaloneExpansion',
	'Mod': 'mod',
	'Episode': 'episode',
	'Season': 'season',
	'Remake': 'remake',
	'Remaster': 'remaster',
	'Expanded Game': 'expandedGame',
	'Port': 'port',
	'Fork': 'fork',
	'Pack': 'addon',
	'Update': 'update',
}

/** IGDB release date region → lexicon region mapping */
export const IGDB_REGION_MAP: Record<number, string> = {
	1: 'europe',
	2: 'northAmerica',
	3: 'australia',
	4: 'newZealand',
	5: 'japan',
	6: 'china',
	7: 'asia',
	8: 'worldwide',
	9: 'korea',
	10: 'brazil',
}

/** IGDB release date category → lexicon releasedAtFormat mapping */
export const IGDB_DATE_FORMAT_MAP: Record<number, string> = {
	0: 'YYYY-MM-DD',
	1: 'YYYY-MM',
	2: 'YYYY',
	3: 'YYYY-Q1',
	4: 'YYYY-Q2',
	5: 'YYYY-Q3',
	6: 'YYYY-Q4',
	7: 'TBD',
}

/** IGDB age rating organization → lexicon organization mapping */
export const IGDB_AGE_RATING_ORG_MAP: Record<number, string> = {
	1: 'esrb',
	2: 'pegi',
	3: 'cero',
	4: 'usk',
	5: 'grac',
	6: 'classInd',
	7: 'acb',
}

export interface MappedAgeRating { organization: string; rating: string }

// Rating labels follow lexicon search.lua's age filters: PEGI as words
// ("Twelve"), USK and ClassInd as plain numbers ("12"), the rest as the
// organisation prints them. GRAC 19+ stays "Nineteen", the label
// stored records already use; IGDB's newer GRAC 18+ is "Eighteen".
const ESRB = 'esrb', PEGI = 'pegi', CERO = 'cero', USK = 'usk', GRAC = 'grac', CLASS_IND = 'classInd', ACB = 'acb'

/**
 * IGDB `age_ratings.rating_category` → organisation and rating. Each id is a
 * row of IGDB's age_rating_categories endpoint, which names its organisation,
 * so the organisation comes from here rather than from a separate field.
 * Checked against the live endpoint on 2026-11-15.
 */
export const IGDB_AGE_RATING_CATEGORY_MAP: Record<number, MappedAgeRating> = {
	1: { organization: ESRB, rating: 'RP' }, 2: { organization: ESRB, rating: 'EC' }, 3: { organization: ESRB, rating: 'E' },
	4: { organization: ESRB, rating: 'E10' }, 5: { organization: ESRB, rating: 'T' }, 6: { organization: ESRB, rating: 'M' },
	7: { organization: ESRB, rating: 'AO' },
	8: { organization: PEGI, rating: 'Three' }, 9: { organization: PEGI, rating: 'Seven' }, 10: { organization: PEGI, rating: 'Twelve' },
	11: { organization: PEGI, rating: 'Sixteen' }, 12: { organization: PEGI, rating: 'Eighteen' },
	13: { organization: CERO, rating: 'A' }, 14: { organization: CERO, rating: 'B' }, 15: { organization: CERO, rating: 'C' },
	16: { organization: CERO, rating: 'D' }, 17: { organization: CERO, rating: 'Z' },
	18: { organization: USK, rating: '0' }, 19: { organization: USK, rating: '6' }, 20: { organization: USK, rating: '12' },
	21: { organization: USK, rating: '16' }, 22: { organization: USK, rating: '18' },
	23: { organization: GRAC, rating: 'All' }, 24: { organization: GRAC, rating: 'Twelve' }, 25: { organization: GRAC, rating: 'Fifteen' },
	26: { organization: GRAC, rating: 'Nineteen' }, 27: { organization: GRAC, rating: 'Testing' }, 40: { organization: GRAC, rating: 'Eighteen' },
	28: { organization: CLASS_IND, rating: 'L' }, 29: { organization: CLASS_IND, rating: '10' }, 30: { organization: CLASS_IND, rating: '12' },
	31: { organization: CLASS_IND, rating: '14' }, 32: { organization: CLASS_IND, rating: '16' }, 33: { organization: CLASS_IND, rating: '18' },
	34: { organization: ACB, rating: 'G' }, 35: { organization: ACB, rating: 'PG' }, 36: { organization: ACB, rating: 'M' },
	37: { organization: ACB, rating: 'MA15' }, 38: { organization: ACB, rating: 'R18' }, 39: { organization: ACB, rating: 'RC' },
}

/**
 * IGDB's deprecated `age_ratings.rating` enum, used only when a rating has no
 * `rating_category`. Its ids are numbered differently: PEGI first (1–5), then
 * ESRB (6–12); from 13 on they line up with the categories.
 */
export const IGDB_LEGACY_AGE_RATING_MAP: Record<number, MappedAgeRating> = {
	1: { organization: PEGI, rating: 'Three' }, 2: { organization: PEGI, rating: 'Seven' }, 3: { organization: PEGI, rating: 'Twelve' },
	4: { organization: PEGI, rating: 'Sixteen' }, 5: { organization: PEGI, rating: 'Eighteen' },
	6: { organization: ESRB, rating: 'RP' }, 7: { organization: ESRB, rating: 'EC' }, 8: { organization: ESRB, rating: 'E' },
	9: { organization: ESRB, rating: 'E10' }, 10: { organization: ESRB, rating: 'T' }, 11: { organization: ESRB, rating: 'M' },
	12: { organization: ESRB, rating: 'AO' },
	...Object.fromEntries(Object.entries(IGDB_AGE_RATING_CATEGORY_MAP).filter(([id]) => Number(id) >= 13 && Number(id) <= 39)),
}
