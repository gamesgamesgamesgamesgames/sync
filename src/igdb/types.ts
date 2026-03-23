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

/** IGDB age rating enum → lexicon rating string mapping */
export const IGDB_AGE_RATING_MAP: Record<number, string> = {
	// PEGI
	1: 'Three', 2: 'Seven', 3: 'Twelve', 4: 'Sixteen', 5: 'Eighteen',
	// ESRB
	6: 'RP', 7: 'EC', 8: 'E', 9: 'E10', 10: 'T', 11: 'M', 12: 'AO',
	// CERO
	13: 'A', 14: 'B', 15: 'C', 16: 'D', 17: 'Z',
	// USK
	18: 'Zero', 19: 'Six', 20: 'Twelve', 21: 'Sixteen', 22: 'Eighteen',
	// GRAC
	23: 'All', 24: 'Twelve', 25: 'Fifteen', 26: 'Nineteen', 27: 'Testing',
	// CLASS_IND
	28: 'L', 29: 'Ten', 30: 'Twelve', 31: 'Fourteen', 32: 'Sixteen', 33: 'Eighteen',
	// ACB
	34: 'G', 35: 'PG', 36: 'M', 37: 'MA15', 38: 'R18', 39: 'RC',
}
