import { afterEach, describe, expect, test, vi } from 'vitest'
import { mapApplicationType } from './mapping.js'
import { IGDB_GAME_CATEGORY_MAP, IGDB_GAME_TYPE_MAP } from '../igdb/types.js'

// games.gamesgamesgamesgames.defs#applicationType, as of 2026-11-15.
const LEXICON_APPLICATION_TYPES = new Set([
	'addon', 'bundle', 'dlc', 'episode', 'expandedGame', 'expansion', 'fork', 'game',
	'mod', 'port', 'remake', 'remaster', 'season', 'standaloneExpansion', 'update',
])

describe('mapApplicationType', () => {
	afterEach(() => { vi.restoreAllMocks() })

	test('IGDB\'s "Pack / Addon" is an addon, and the older "Pack" still is', () => {
		expect(mapApplicationType({ id: 1, game_type: { id: 13, type: 'Pack / Addon' } })).toBe('addon')
		expect(mapApplicationType({ id: 1, game_type: { id: 13, type: 'Pack' } })).toBe('addon')
	})

	test('a known game_type wins over category', () => {
		expect(mapApplicationType({ id: 1, game_type: { id: 1, type: 'DLC' }, category: 0 })).toBe('dlc')
	})

	test('an unknown game_type is left unset rather than guessed from category, with one warning per type', () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
		expect(mapApplicationType({ id: 1, game_type: { id: 99, type: 'Brand New Kind' } })).toBeUndefined()
		expect(mapApplicationType({ id: 2, game_type: { id: 99, type: 'Brand New Kind' }, category: 0 })).toBeUndefined()
		expect(warn).toHaveBeenCalledTimes(1)
		expect(warn.mock.calls[0]![0]).toMatch(/Brand New Kind.*game 1/)
	})

	test('without a game_type, the category enum still applies', () => {
		expect(mapApplicationType({ id: 1, category: 13 })).toBe('addon')
		expect(mapApplicationType({ id: 1 })).toBe('game')
	})

	test('every mapped value is one the lexicon allows', () => {
		for (const value of [...Object.values(IGDB_GAME_TYPE_MAP), ...Object.values(IGDB_GAME_CATEGORY_MAP)]) {
			expect(LEXICON_APPLICATION_TYPES.has(value), value).toBe(true)
		}
	})
})
