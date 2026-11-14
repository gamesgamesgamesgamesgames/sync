const CONTRIBUTABLE_FIELDS = new Set([
	'name',
	'summary',
	'applicationType',
	'genres',
	'modes',
	'themes',
	'playerPerspectives',
	'releases',
	'media',
	'parent',
	'storyline',
	'keywords',
	'websites',
	'videos',
	'alternativeNames',
	'timeToBeat',
	'ageRatings',
	'languageSupports',
	'multiplayerModes',
	'engines',
	'externalIds',
	'description',
	'descriptionFacets',
	'systemRequirements',
	'platformFeatures',
])

function deepEqual(a: unknown, b: unknown): boolean {
	if (a === b) return true
	if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false
	if (Array.isArray(a) !== Array.isArray(b)) return false

	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) return false
		return a.every((item, i) => deepEqual(item, b[i]))
	}

	const aObj = a as Record<string, unknown>
	const bObj = b as Record<string, unknown>
	const aKeys = Object.keys(aObj).sort()
	const bKeys = Object.keys(bObj).sort()

	if (aKeys.length !== bKeys.length) return false
	return aKeys.every((key, i) => bKeys[i] === key && deepEqual(aObj[key], bObj[key]))
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
	return v !== null && typeof v === 'object' && !Array.isArray(v)
}

/**
 * IGDB only knows some services, so its external ids go over the stored ones
 * instead of replacing them. A Twitch id resolved elsewhere, say, survives.
 */
export function mergeExternalIds(incoming: unknown, stored: unknown): unknown {
	if (!isPlainObject(incoming) || !isPlainObject(stored)) return incoming
	return { ...stored, ...incoming }
}

/**
 * Keep only the fields a contribution may carry. createContribution rejects
 * anything else, so a newGame built from the full mapped record (with
 * `$type`, `createdAt`, `publishedAt`) would always be refused.
 */
export function pickContributableFields(mapped: Record<string, unknown>): Record<string, unknown> {
	const picked: Record<string, unknown> = {}
	for (const field of CONTRIBUTABLE_FIELDS) {
		if (field in mapped && mapped[field] !== undefined) {
			picked[field] = mapped[field]
		}
	}
	return picked
}

/**
 * Compare a mapped IGDB record against the current game record.
 * Returns only the contributable fields that differ.
 */
export function diffGameRecord(
	mapped: Record<string, unknown>,
	current: Record<string, unknown>,
): Record<string, unknown> {
	const changes: Record<string, unknown> = {}

	for (const field of CONTRIBUTABLE_FIELDS) {
		if (!(field in mapped)) continue
		let value = mapped[field]
		if (field === 'externalIds') {
			value = mergeExternalIds(value, current[field])
		}
		if (deepEqual(value, current[field])) continue
		changes[field] = value
	}

	return changes
}
