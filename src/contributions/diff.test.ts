import { describe, test, expect } from 'vitest'
import { diffGameRecord } from './diff.js'

describe('diffGameRecord', () => {
	test('returns empty object when records are identical', () => {
		const mapped = { name: 'Portal', applicationType: 'game', externalIds: { igdb: '123' } }
		const current = { name: 'Portal', applicationType: 'game', externalIds: { igdb: '123' } }
		expect(diffGameRecord(mapped, current)).toEqual({})
	})

	test('returns only changed scalar fields', () => {
		const mapped = { name: 'Portal 2', applicationType: 'game', summary: 'A sequel' }
		const current = { name: 'Portal', applicationType: 'game', summary: 'A puzzle game' }
		expect(diffGameRecord(mapped, current)).toEqual({
			name: 'Portal 2',
			summary: 'A sequel',
		})
	})

	test('returns changed array fields', () => {
		const mapped = { name: 'Portal', genres: ['puzzle', 'platformer'] }
		const current = { name: 'Portal', genres: ['puzzle'] }
		expect(diffGameRecord(mapped, current)).toEqual({
			genres: ['puzzle', 'platformer'],
		})
	})

	test('returns changed nested object fields', () => {
		const mapped = { name: 'Portal', externalIds: { igdb: '123', steam: '400' } }
		const current = { name: 'Portal', externalIds: { igdb: '123' } }
		expect(diffGameRecord(mapped, current)).toEqual({
			externalIds: { igdb: '123', steam: '400' },
		})
	})

	test('ignores fields not in CONTRIBUTABLE_FIELDS', () => {
		const mapped = { name: 'Portal', $type: 'games.gamesgamesgamesgames.game', createdAt: '2024-01-01' }
		const current = { name: 'Portal', $type: 'games.gamesgamesgamesgames.game', createdAt: '2023-01-01' }
		expect(diffGameRecord(mapped, current)).toEqual({})
	})

	test('includes new fields not present on current record', () => {
		const mapped = { name: 'Portal', summary: 'A puzzle game' }
		const current = { name: 'Portal' }
		expect(diffGameRecord(mapped, current)).toEqual({
			summary: 'A puzzle game',
		})
	})

	test('ignores fields present on current but absent in mapped', () => {
		const mapped = { name: 'Portal' }
		const current = { name: 'Portal', summary: 'A puzzle game' }
		expect(diffGameRecord(mapped, current)).toEqual({})
	})
})

describe('diffGameRecord externalIds', () => {
	test('merges IGDB ids over the stored ones instead of replacing them', () => {
		const mapped = { externalIds: { igdb: '1', steam: '400' } }
		const current = { externalIds: { igdb: '1', twitch: '555' } }
		expect(diffGameRecord(mapped, current)).toEqual({
			externalIds: { igdb: '1', twitch: '555', steam: '400' },
		})
	})

	test('ids that add nothing new are not a change', () => {
		const mapped = { externalIds: { igdb: '1' } }
		const current = { externalIds: { igdb: '1', twitch: '555' } }
		expect(diffGameRecord(mapped, current)).toEqual({})
	})

	test('an IGDB value overrides a stored one for the same service', () => {
		const mapped = { externalIds: { steam: '401' } }
		const current = { externalIds: { steam: '400', twitch: '555' } }
		expect(diffGameRecord(mapped, current)).toEqual({ externalIds: { steam: '401', twitch: '555' } })
	})
})
