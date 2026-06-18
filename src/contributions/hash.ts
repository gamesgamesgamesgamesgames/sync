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

export function contentHashRkey(changes: Record<string, unknown>): string {
	const serialized = stableStringify(changes)
	return createHash('sha256').update(serialized).digest('hex').slice(0, 32)
}
