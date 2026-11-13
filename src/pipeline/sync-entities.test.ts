import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from '../state.js'
import { ContributionRequestError, type ContributionClient } from '../contributions/client.js'
import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import { classifyFailure, isPermanentFailure, syncEntityType } from './sync-entities.js'

const PREVIOUS_CURSOR = 1_781_308_800 // 2026-06-13

function fakeIgdb(ids: number[]): IGDBClient {
	return {
		async *paginate() {
			yield { items: ids.map((id) => ({ id })), offset: 0 }
		},
	} as unknown as IGDBClient
}

function contributionClient(createContribution: (params: { changes: Record<string, unknown> }) => Promise<{ uri: string }>): ContributionClient {
	return {
		getGameByIgdbId: vi.fn(async () => null),
		getGameRecord: vi.fn(async () => null),
		getRecord: vi.fn(async () => null),
		createContribution: vi.fn(createContribution),
	} as unknown as ContributionClient
}

describe('syncEntityType cursor', () => {
	let dbPath: string
	let state: StateManager

	beforeEach(() => {
		dbPath = join(tmpdir(), `sync-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		state = new StateManager(dbPath)
		state.setLastSyncAt('game', PREVIOUS_CURSOR)
		vi.spyOn(console, 'log').mockImplementation(() => {})
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		vi.spyOn(console, 'error').mockImplementation(() => {})
	})

	afterEach(() => {
		vi.restoreAllMocks()
		state.close()
		for (const suffix of ['', '-wal', '-shm']) {
			const file = dbPath + suffix
			if (existsSync(file)) unlinkSync(file)
		}
	})

	function run(client: ContributionClient, ids = [1, 2, 3], extra: Partial<Parameters<typeof syncEntityType>[0]> = {}) {
		return syncEntityType<{ id: number }>({
			entityType: 'game',
			igdbEndpoint: 'games',
			igdbFields: 'fields name;',
			collection: 'games.gamesgamesgamesgames.game',
			contributionClient: client,
			mapRecord: async (item) => ({ name: `Game ${item.id}`, applicationType: 'game' }),
			...extra,
		} as Parameters<typeof syncEntityType<{ id: number }>>[0], fakeIgdb(ids), {} as AtprotoClient, state)
	}

	const ok = () => contributionClient(async () => ({ uri: 'at://x/c/1' }))
	const failGame = (id: number, err: () => Error) => contributionClient(async (params) => {
		if ((params as { changes: { name?: string } }).changes.name === `Game ${id}`) throw err()
		return { uri: 'at://x/c/1' }
	})

	test('a game that keeps failing with a 500 stops holding the cursor after 3 runs', async () => {
		const lua500 = () => new ContributionRequestError('createContribution', 500, 'script error')
		for (let run_ = 1; run_ <= 2; run_++) {
			vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + run_ * 86400) * 1000)
			const r = await run(failGame(2, lua500))
			expect(r.cursorAdvanced).toBe(false)
			expect(state.getFailureStreak('game', '2')).toBe(run_)
		}
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 3 * 86400) * 1000)
		const third = await run(failGame(2, lua500))
		expect(third.givenUp).toBe(1)
		expect(third.cursorAdvanced).toBe(true)
		expect(state.getFailures('game')).toEqual(['2'])
		expect(state.getFailureStreak('game', '2')).toBe(0)
	})

	test('a streak resets when the game succeeds', async () => {
		const lua500 = () => new ContributionRequestError('createContribution', 500, 'script error')
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		await run(failGame(2, lua500))
		expect(state.getFailureStreak('game', '2')).toBe(1)
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 2 * 86400) * 1000)
		await run(ok())
		expect(state.getFailureStreak('game', '2')).toBe(0)
	})

	test('auth failures never give up on a game, however many runs', async () => {
		const auth = () => new ContributionRequestError('createContribution', 401, 'no DPoP key matching proof thumbprint')
		for (let run_ = 1; run_ <= 5; run_++) {
			vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + run_ * 86400) * 1000)
			const r = await run(failGame(2, auth))
			expect(r.cursorAdvanced).toBe(false)
			expect(r.givenUp).toBe(0)
		}
		expect(state.getFailures('game')).toEqual([])
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('a mapRecord rejection holds the cursor and counts against that game', async () => {
		const r = await run(ok(), [1, 2], {
			mapRecord: async (item: { id: number }) => {
				if (item.id === 2) throw new Error('IGDB image download exploded')
				return { name: `Game ${item.id}`, applicationType: 'game' }
			},
		})
		expect(r.failed).toBe(1)
		expect(r.cursorAdvanced).toBe(false)
		expect(state.getFailureStreak('game', '2')).toBe(1)
	})

	test('a failed direct put holds the cursor', async () => {
		state.setEntity('platform', '7', 'at://did:web:x/games.gamesgamesgamesgames.platform/p7')
		state.setLastSyncAt('platform', PREVIOUS_CURSOR)
		const atproto = {
			getRecord: vi.fn(async () => ({ name: 'Old' })),
			applyUpdates: vi.fn(async () => { throw new Error('batch rejected') }),
			putRecord: vi.fn(async () => { throw new Error('PDS 500') }),
		} as unknown as AtprotoClient
		const r = await syncEntityType<{ id: number }>({
			entityType: 'platform',
			igdbEndpoint: 'platforms',
			igdbFields: 'fields name;',
			collection: 'games.gamesgamesgamesgames.platform',
			mapRecord: async () => ({ name: 'New' }),
		}, fakeIgdb([7]), atproto, state)
		expect(r.failed).toBe(1)
		expect(r.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('platform')).toBe(PREVIOUS_CURSOR)
	})

	test('warns when the cursor is older than the alert threshold', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 4 * 86400) * 1000)
		await run(ok())
		expect(warn.mock.calls.some((c) => String(c[0]).includes('[ALERT]'))).toBe(true)
	})

	test('advances the cursor when every game succeeds', async () => {
		const result = await run(contributionClient(async () => ({ uri: 'at://x/c/1' })))

		expect(result.created).toBe(3)
		expect(result.cursorAdvanced).toBe(true)
		expect(state.getLastSyncAt('game')).toBeGreaterThan(PREVIOUS_CURSOR)
	})

	test('keeps the cursor when a game fails with a rate limit', async () => {
		let n = 0
		const client = contributionClient(async () => {
			n++
			if (n === 2) throw new ContributionRequestError('createContribution', 429, 'RateLimited')
			return { uri: `at://x/c/${n}` }
		})

		const result = await run(client)

		expect(result.failed).toBe(1)
		expect(result.retryable).toBe(1)
		expect(result.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('keeps the cursor on an auth failure', async () => {
		const client = contributionClient(async () => {
			throw new ContributionRequestError('createContribution', 401, 'no DPoP key matching proof thumbprint')
		})

		const result = await run(client)

		expect(result.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('keeps the cursor on an unexpected error from our own code', async () => {
		const client = contributionClient(async () => {
			throw new TypeError("Cannot read properties of undefined (reading 'uri')")
		})

		const result = await run(client)

		expect(result.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('records a permanent refusal for review and still advances', async () => {
		let n = 0
		const client = contributionClient(async () => {
			n++
			if (n === 1) throw new ContributionRequestError('createContribution', 400, 'invalid field in changes: foo')
			return { uri: `at://x/c/${n}` }
		})

		const result = await run(client)

		expect(result.failed).toBe(1)
		expect(result.retryable).toBe(0)
		expect(result.cursorAdvanced).toBe(true)
		expect(state.getFailures('game')).toEqual(['1'])
	})
})

describe('classifyFailure', () => {
	test.each([
		[400, 'permanent'],
		[404, 'permanent'],
		[401, 'systemic'],
		[403, 'systemic'],
		[408, 'systemic'],
		[429, 'systemic'],
		[500, 'entity'],
		[502, 'entity'],
	])('status %i → %s', (status, kind) => {
		expect(classifyFailure(new ContributionRequestError('x', status, ''))).toBe(kind)
	})

	test('network errors are systemic', () => {
		expect(classifyFailure(new TypeError('fetch failed'))).toBe('systemic')
	})

	test('our own bugs are per-entity', () => {
		expect(classifyFailure(new TypeError("Cannot read properties of undefined (reading 'uri')"))).toBe('entity')
	})
})

describe('isPermanentFailure', () => {
	test.each([
		[400, true],
		[404, true],
		[422, true],
		[401, false],
		[403, false],
		[408, false],
		[429, false],
		[500, false],
		[503, false],
	])('status %i → %s', (status, expected) => {
		expect(isPermanentFailure(new ContributionRequestError('x', status, ''))).toBe(expected)
	})

	test('other errors are not permanent', () => {
		expect(isPermanentFailure(new Error('fetch failed'))).toBe(false)
	})
})
