import { describe, test, expect } from 'vitest'
import { contentHashRkey } from './hash.js'

describe('contentHashRkey', () => {
	// Golden value from before mirror state existed: a correction with no
	// mirrored media must keep exactly this key, so existing contributions
	// still dedupe.
	test('an empty mirrored list leaves the key unchanged', () => {
		expect(contentHashRkey({ summary: 'new' }, 'bafyBase')).toBe('2befc593a0179ed120773939899548d7')
		expect(contentHashRkey({ summary: 'new' }, 'bafyBase', [])).toBe('2befc593a0179ed120773939899548d7')
	})

	test('mirrored CIDs change the key, in any order', () => {
		const k = contentHashRkey({ summary: 'new' }, 'bafyBase', ['b', 'a'])
		expect(k).not.toBe('2befc593a0179ed120773939899548d7')
		expect(contentHashRkey({ summary: 'new' }, 'bafyBase', ['a', 'b'])).toBe(k)
	})
})
