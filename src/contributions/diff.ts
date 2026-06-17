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
		if (deepEqual(mapped[field], current[field])) continue
		changes[field] = mapped[field]
	}

	return changes
}
