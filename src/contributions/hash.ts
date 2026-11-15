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
 * key. newGame contributions have no base.
 */
export function contentHashRkey(changes: Record<string, unknown>, baseCid?: string): string {
	const serialized = baseCid === undefined ? stableStringify(changes) : stableStringify({ base: baseCid, changes })
	return createHash('sha256').update(serialized).digest('hex').slice(0, 32)
}
