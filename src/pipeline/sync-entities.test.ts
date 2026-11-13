import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from '../state.js'
import { ContributionRequestError, type ContributionClient } from '../contributions/client.js'
import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import { isPermanentFailure, syncEntityType } from './sync-entities.js'

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

	function run(client: ContributionClient, ids = [1, 2, 3]) {
		return syncEntityType<{ id: number }>({
			entityType: 'game',
			igdbEndpoint: 'games',
			igdbFields: 'fields name;',
			collection: 'games.gamesgamesgamesgames.game',
			contributionClient: client,
			mapRecord: async (item) => ({ name: `Game ${item.id}`, applicationType: 'game' }),
		}, fakeIgdb(ids), {} as AtprotoClient, state)
	}

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
