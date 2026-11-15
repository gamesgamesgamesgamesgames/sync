import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from './state.js'
import type { IGDBClient } from './igdb/client.js'
import type { AtprotoClient } from './atproto/client.js'
import type { ContributionClient } from './contributions/client.js'
import { syncEntityType } from './pipeline/sync-entities.js'
import { heldGamesReport, heldGamesToRecheck, trackHeldGames } from './held-games.js'

describe('held games', () => {
	let dbPath: string
	let state: StateManager

	beforeEach(() => {
		dbPath = join(tmpdir(), `held-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		state = new StateManager(dbPath)
		vi.spyOn(console, 'log').mockImplementation(() => {})
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		vi.spyOn(console, 'error').mockImplementation(() => {})
	})

	afterEach(() => {
		vi.restoreAllMocks()
		state.close()
		for (const suffix of ['', '-wal', '-shm']) {
			if (existsSync(dbPath + suffix)) unlinkSync(dbPath + suffix)
		}
	})

	test('a held new game is recorded with its game_type, and the cursor still moves', async () => {
		state.setLastSyncAt('game', 1_781_308_800)
		const types: Record<string, string> = { '1': 'Brand New Kind', '2': 'Main Game' }
		const client = {
			getGameByIgdbId: vi.fn(async () => null),
			getRecord: vi.fn(async () => null),
			createContribution: vi.fn(async () => ({ uri: 'at://x/c/1' })),
		} as unknown as ContributionClient
		const igdb = { async *paginate() { yield { items: [{ id: 1 }, { id: 2 }], offset: 0 } } } as unknown as IGDBClient
		const r = await syncEntityType<{ id: number }>({
			entityType: 'game',
			igdbEndpoint: 'games',
			igdbFields: 'fields name;',
			collection: 'games.gamesgamesgamesgames.game',
			contributionClient: client,
			onContributionResult: trackHeldGames(state, (key) => types[key]),
			// Game 1's type doesn't map, so it has no applicationType.
			mapRecord: async (item) => item.id === 1 ? { name: 'Held' } : { name: 'Fine', applicationType: 'game' },
		}, igdb, {} as AtprotoClient, state)
		expect(r.cursorAdvanced).toBe(true)
		expect(client.createContribution).toHaveBeenCalledTimes(1)
		expect(state.getHeldGames()).toEqual([{ igdbId: 1, gameType: 'Brand New Kind', heldAt: expect.any(Number) }])
	})

	test('any other outcome releases a held game', () => {
		state.holdGame(1, 'Brand New Kind')
		const track = trackHeldGames(state, () => 'Brand New Kind')
		track('1', 'held')
		expect(state.getHeldGames()).toHaveLength(1)
		track('1', 'created')
		expect(state.getHeldGames()).toHaveLength(0)
		state.holdGame(2, 'X')
		track('2', 'skipped')
		expect(state.getHeldGames()).toHaveLength(0)
	})

	test('only held games whose type maps now are re-checked', () => {
		const held = [
			{ igdbId: 1, gameType: 'Pack / Addon' },
			{ igdbId: 2, gameType: 'Brand New Kind' },
			{ igdbId: 3, gameType: null },
		]
		expect(heldGamesToRecheck(held)).toEqual([1])
		expect(heldGamesToRecheck(held, { 'Brand New Kind': 'game' })).toEqual([2])
	})

	test('held games need attention and come with a rerun list', () => {
		expect(heldGamesReport([])).toBeNull()
		const report = heldGamesReport([{ igdbId: 7, gameType: 'B' }, { igdbId: 9, gameType: 'A' }, { igdbId: 11, gameType: 'A' }])!
		expect(report.attention).toMatch(/^3 new game\(s\) held for unmapped IGDB game_type\(s\) "A", "B"/)
		expect(report.rerun).toBe('7\n9\n11')
	})
})
