import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from './state.js'

describe('StateManager — steam_map', () => {
	let dbPath: string
	let state: StateManager

	beforeEach(() => {
		dbPath = join(tmpdir(), `state-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		state = new StateManager(dbPath)
	})

	afterEach(() => {
		state.close()
		for (const suffix of ['', '-wal', '-shm']) {
			const file = dbPath + suffix
			if (existsSync(file)) unlinkSync(file)
		}
	})

	it('should set and get a steam mapping', () => {
		state.setSteamMapping('123', '456', 'at://did:plc:abc/games.gamesgamesgamesgames.game/tid')
		const mapping = state.getSteamMapping('123')
		expect(mapping).toEqual({
			igdbId: '123',
			steamId: '456',
			atUri: 'at://did:plc:abc/games.gamesgamesgamesgames.game/tid',
			enrichedAt: null,
		})
	})

	it('should return undefined for a missing mapping', () => {
		const mapping = state.getSteamMapping('nonexistent')
		expect(mapping).toBeUndefined()
	})

	it('should return unenriched mappings', () => {
		state.setSteamMapping('1', 's1', 'at://1')
		state.setSteamMapping('2', 's2', 'at://2')
		state.setSteamMapping('3', 's3', 'at://3')

		const unenriched = state.getUnenrichedSteamMappings(10)
		expect(unenriched).toHaveLength(3)
	})

	it('should respect limit on unenriched query', () => {
		state.setSteamMapping('1', 's1', 'at://1')
		state.setSteamMapping('2', 's2', 'at://2')
		state.setSteamMapping('3', 's3', 'at://3')

		const unenriched = state.getUnenrichedSteamMappings(2)
		expect(unenriched).toHaveLength(2)
	})

	it('should exclude enriched mappings from unenriched query', () => {
		state.setSteamMapping('1', 's1', 'at://1')
		state.setSteamMapping('2', 's2', 'at://2')
		state.markSteamEnriched('1')

		const unenriched = state.getUnenrichedSteamMappings(10)
		expect(unenriched).toHaveLength(1)
		expect(unenriched[0].igdbId).toBe('2')
	})

	it('should preserve enrichedAt when updating a mapping', () => {
		state.setSteamMapping('1', 's1', 'at://1')
		state.markSteamEnriched('1')

		const before = state.getSteamMapping('1')
		expect(before?.enrichedAt).toBeTypeOf('number')

		// Update the mapping
		state.setSteamMapping('1', 's1-updated', 'at://1-updated')

		const after = state.getSteamMapping('1')
		expect(after?.steamId).toBe('s1-updated')
		expect(after?.atUri).toBe('at://1-updated')
		expect(after?.enrichedAt).toBe(before?.enrichedAt)
	})
})
