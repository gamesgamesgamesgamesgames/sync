import { createHash } from 'node:crypto'

function stableStringify(value: unknown): string {
	if (value === null || value === undefined) return 'null'
	if (typeof value !== 'object') return JSON.stringify(value)

	if (Array.isArray(value)) {
		return '[' + value.map(stableStringify).join(',') + ']'
	}

	const sorted = Object.keys(value as Record<string, unknown>).sort()
	const entries = sorted.map(
		(k) => JSON.stringify(k) + ':' + stableStringify((value as Record<string, unknown>)[k]),
	)
	return '{' + entries.join(',') + '}'
}

/**
 * A record key derived from the contribution's content, so resubmitting the
 * same contribution is a no-op. Corrections pass the CID of the record they
 * were diffed against, so the same diff against a different base gets a new
 * key, and the sorted CIDs of their media already mirrored to the catalogue
 * repo, so a media change gets a new key once its blobs are there. newGame
 * contributions pass neither. With no mirrored media the key is the same as
 * before that input existed.
 */
export function contentHashRkey(changes: Record<string, unknown>, baseCid?: string, mirroredCids: string[] = []): string {
	let input: unknown = changes
	if (baseCid !== undefined) input = { base: baseCid, changes }
	if (mirroredCids.length > 0) input = { ...(input === changes ? { changes } : input as object), mirrored: [...mirroredCids].sort() }
	return createHash('sha256').update(stableStringify(input)).digest('hex').slice(0, 32)
}
