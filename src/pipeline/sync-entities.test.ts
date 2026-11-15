import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from '../state.js'
import { ContributionRequestError, RecordReadError, type ContributionClient } from '../contributions/client.js'
import { RecordUnreadableError } from '../contributions/handler.js'
import { PdsResolutionError } from '../atproto/pds-resolver.js'
import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import { IGDB_IDS_PAGE_SIZE, breakerTrips, classifyFailure, errorShape, igdbIdWhereClauses, isPermanentFailure, needsAttention, syncEntityType } from './sync-entities.js'

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

	function run(client: ContributionClient, ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], extra: Partial<Parameters<typeof syncEntityType>[0]> = {}) {
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
		const r = await run(ok(), undefined, {
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

	const range = (n: number) => Array.from({ length: n }, (_, i) => i + 1)
	const nameId = (params: unknown) => Number((params as { changes: { name: string } }).changes.name.split(' ')[1])

	/** A client whose createContribution fails for the games `fail` picks. */
	function writingClient(fail: (id: number) => Error | null): ContributionClient {
		return {
			getGameByIgdbId: vi.fn(async () => null),
			getGameRecord: vi.fn(async () => null),
			getRecord: vi.fn(async () => null),
			createContribution: vi.fn(async (params: unknown) => {
				const err = fail(nameId(params))
				if (err) throw err
				return { uri: 'at://x/c/1' }
			}),
		} as unknown as ContributionClient
	}

	function skippingClient(fail: (id: number) => Error | null, skip: (id: number) => boolean): ContributionClient {
		const client = writingClient(fail)
		// getRecord finding the content-hash rkey means "already submitted": skip without writing.
		let current = 0
		;(client as unknown as { getRecord: unknown }).getRecord = vi.fn(async () => (skip(current) ? { exists: true } : null))
		;(client as unknown as { getGameByIgdbId: unknown }).getGameByIgdbId = vi.fn(async (igdbId: string) => { current = Number(igdbId); return null })
		return client
	}

	test('the breaker divides by write attempts: 6 failures among 6 writes trips it, though 50 games were seen', async () => {
		const lua500 = (id: number) => new ContributionRequestError('createContribution', 500, `script error at game ${id}: field ${id} invalid`)
		const client = skippingClient((id) => (id <= 6 ? lua500(id) : null), (id) => id > 6)
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const r = await run(client, range(50))
		expect(client.createContribution).toHaveBeenCalledTimes(6)
		expect(r.systemic).toBe(1)
		expect(state.getFailureStreak('game', '1')).toBe(0)
	})

	test('a few failures among many writes does not trip it, and the games are charged', async () => {
		const client = writingClient((id) => (id <= 6 ? new ContributionRequestError('createContribution', 500, `boom ${id} at://did:web:x/g/${id}`) : null))
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const r = await run(client, range(50))
		expect(r.systemic).toBe(0)
		expect(state.getFailureStreak('game', '1')).toBe(1)
	})

	test('many failures sharing one error trip it even at a low ratio', async () => {
		const same = (id: number) => new ContributionRequestError('createContribution', 500, `{"error":"InternalServerError","message":"script error: attempt to index a nil value (field 'changes') for game ${id}"}`)
		const client = writingClient((id) => (id <= 12 ? same(id) : null))
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const r = await run(client, range(100))
		expect(r.systemic).toBe(1)
		expect(state.getFailureStreak('game', '1')).toBe(0)
	})

	test('as many failures with different errors at a low ratio do not trip it', async () => {
		const distinct = ['timeout reading body', 'invalid releases', 'name too long', 'bad media', 'unknown platform', 'duplicate slug', 'parent missing', 'engine missing', 'bad rating', 'bad website', 'bad video', 'bad keywords']
		const client = writingClient((id) => (id <= 12 ? new ContributionRequestError('createContribution', 500, distinct[id - 1]!) : null))
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const r = await run(client, range(100))
		expect(r.systemic).toBe(0)
		expect(state.getFailureStreak('game', '12')).toBe(1)
	})

	const day = (n: number) => vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + n * 86400) * 1000)
	const clusterError = (id: number) => new ContributionRequestError('createContribution', 500, `script error: unknown applicationType for game ${id}`)

	test('the breaker exempts a new cluster for 3 tripped runs, then charges it and the cursor moves on', async () => {
		// 6 games fail the same way; the other 44 already have their contribution and skip before writing.
		const client = skippingClient((id) => (id <= 6 ? clusterError(id) : null), (id) => id > 6)
		for (let n = 1; n <= 3; n++) {
			day(n)
			const r = await run(client, range(50))
			expect(r.systemic).toBe(1)
			expect(state.getFailureStreak('game', '1')).toBe(0)
		}
		const err = vi.spyOn(console, 'error').mockImplementation(() => {})
		day(4)
		let r = await run(client, range(50))
		expect(err.mock.calls.some((c) => String(c[0]).includes('[BREAKER STUCK]'))).toBe(true)
		expect(state.getFailureStreak('game', '1')).toBe(1)
		day(5); await run(client, range(50))
		day(6); r = await run(client, range(50))
		expect(r.givenUp).toBe(6)
		expect(r.cursorAdvanced).toBe(true)
		expect(state.getFailures('game').sort()).toEqual(['1', '2', '3', '4', '5', '6'])
	})

	test('a game already failing before the breaker trips keeps its streak moving', async () => {
		// Night 1: only game 1 fails. No trip; it gets a streak.
		const lone = skippingClient((id) => (id === 1 ? new ContributionRequestError('createContribution', 500, 'bad releases') : null), () => false)
		day(1)
		const first = await run(lone, range(50))
		expect(first.systemic).toBe(0)
		expect(state.getFailureStreak('game', '1')).toBe(1)
		// Nights 2-3: a cluster appears and trips the breaker; game 1 is still failing.
		const cluster = skippingClient((id) => (id <= 7 ? clusterError(id) : null), (id) => id > 7)
		day(2); await run(cluster, range(50))
		expect(state.getFailureStreak('game', '1')).toBe(2)
		expect(state.getFailureStreak('game', '2')).toBe(0)
		day(3); const third = await run(cluster, range(50))
		expect(third.systemic).toBe(1)
		expect(state.getFailures('game')).toEqual(['1'])
		expect(state.getFailureStreak('game', '2')).toBe(0)
	})

	test('a permanent failure clears the game\'s streak', async () => {
		let status = 500
		const client = writingClient((id) => (id === 1 ? new ContributionRequestError('createContribution', status, 'refused') : null))
		day(1); await run(client)
		expect(state.getFailureStreak('game', '1')).toBe(1)
		status = 400
		day(2); await run(client)
		expect(state.getFailures('game')).toEqual(['1'])
		expect(state.getFailureStreak('game', '1')).toBe(0)
	})

	/** getGame finds every game; the stored-record read fails for the ids in `unreadable`. */
	function readClient(unreadable: (id: number) => boolean): ContributionClient {
		return {
			getGameByIgdbId: vi.fn(async (igdbId: string) => ({ uri: `at://did:web:g/games.gamesgamesgamesgames.game/${igdbId}`, record: {} })),
			getGameRecord: vi.fn(async (uri: string) => {
				const id = Number(uri.split('/').pop())
				return unreadable(id) ? null : { uri, cid: `bafy${id}`, value: { name: 'old' } }
			}),
			getRecord: vi.fn(async () => null),
			canReadRepo: vi.fn(() => true),
			createContribution: vi.fn(async () => ({ uri: 'at://x/c/1' })),
		} as unknown as ContributionClient
	}

	test('one unreadable game among many is charged, and after 3 runs the cursor moves past it', async () => {
		const client = readClient((id) => id === 7)
		for (let n = 1; n <= 2; n++) {
			day(n)
			const r = await run(client, range(50))
			expect(r.systemic).toBe(0)
			expect(r.cursorAdvanced).toBe(false)
			expect(state.getFailureStreak('game', '7')).toBe(n)
		}
		day(3)
		const third = await run(client, range(50))
		expect(third.givenUp).toBe(1)
		expect(third.cursorAdvanced).toBe(true)
		expect(state.getFailures('game')).toEqual(['7'])
	})

	test('every game unreadable (a broken read path) trips the breaker, and no game is charged while it holds', async () => {
		const client = readClient(() => true)
		for (let n = 1; n <= 3; n++) {
			day(n)
			const r = await run(client, range(50))
			expect(r.systemic).toBe(1)
			expect(r.givenUp).toBe(0)
			expect(r.cursorAdvanced).toBe(false)
		}
		expect(state.getFailureStreak('game', '1')).toBe(0)
		expect(state.getFailures('game')).toEqual([])
		expect(client.createContribution).not.toHaveBeenCalled()
	})

	test('the pipeline passes isImageMirrored through to correction rkeys', async () => {
		const media = [{ blob: { $type: 'blob', ref: { $link: 'bafyM' }, mimeType: 'image/jpeg', size: 1 }, mediaType: 'cover', igdbImageId: 'coM' }]
		const rkeys: string[] = []
		const client = {
			getGameByIgdbId: vi.fn(async (id: string) => ({ uri: `at://did:web:g/games.gamesgamesgamesgames.game/${id}`, record: {} })),
			getGameRecord: vi.fn(async (uri: string) => ({ uri, cid: 'bafyBase', value: {} })),
			getRecord: vi.fn(async () => null),
			canReadRepo: vi.fn(() => true),
			createContribution: vi.fn(async (p: { rkey: string }) => { rkeys.push(p.rkey); return { uri: 'at://x/c/1' } }),
		} as unknown as ContributionClient
		const mapRecord = async () => ({ name: 'x', media })
		day(1); await run(client, [1], { mapRecord, isImageMirrored: () => false })
		day(2); await run(client, [1], { mapRecord, isImageMirrored: () => true })
		expect(rkeys).toHaveLength(2)
		expect(rkeys[0]).not.toBe(rkeys[1])
	})

	test('a run that does not trip resets the trip count', async () => {
		const cluster = skippingClient((id) => (id <= 6 ? clusterError(id) : null), (id) => id > 6)
		day(1); await run(cluster, range(50))
		day(2); await run(cluster, range(50))
		day(3); await run(ok())
		expect(state.getMetaNumber('breaker_trips_game')).toBe(0)
		day(4); const r = await run(cluster, range(50))
		expect(r.systemic).toBe(1)
		expect(state.getFailureStreak('game', '1')).toBe(0)
	})

	test('a 502 from HappyView is systemic and counts against no game', async () => {
		const gateway = () => new ContributionRequestError('getGame', 502, 'Bad Gateway')
		for (let run_ = 1; run_ <= 4; run_++) {
			vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + run_ * 86400) * 1000)
			const r = await run(failGame(2, gateway))
			expect(r.systemic).toBe(1)
			expect(needsAttention(r)).toBe(true)
			expect(r.cursorAdvanced).toBe(false)
		}
		expect(state.getFailureStreak('game', '2')).toBe(0)
	})

	test('the first 401 from createContribution stops the run', async () => {
		const client = contributionClient(async () => {
			throw new ContributionRequestError('createContribution', 401, 'no DPoP key matching proof thumbprint')
		})
		const r = await run(client)
		expect(r.aborted).toMatch(/refused the sync account/)
		expect(client.createContribution).toHaveBeenCalledTimes(1)
		expect(r.cursorAdvanced).toBe(false)
		expect(needsAttention(r)).toBe(true)
	})

	test('a 403 from createContribution also stops the run', async () => {
		const client = contributionClient(async () => {
			throw new ContributionRequestError('createContribution', 403, 'forbidden')
		})
		const r = await run(client)
		expect(r.aborted).toBeDefined()
		expect(client.createContribution).toHaveBeenCalledTimes(1)
	})

	test('a clean run needs no attention; a stale cursor does', async () => {
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const clean = await run(ok())
		expect(needsAttention(clean)).toBe(false)
		state.setLastSyncAt('game', PREVIOUS_CURSOR)
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 4 * 86400) * 1000)
		const stale = await run(ok())
		expect(stale.cursorAlert).toBe(true)
		expect(needsAttention(stale)).toBe(true)
	})

	test('warns when the cursor is older than the alert threshold', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 4 * 86400) * 1000)
		await run(ok())
		expect(warn.mock.calls.some((c) => String(c[0]).includes('[ALERT]'))).toBe(true)
	})

	test('advances the cursor when every game succeeds', async () => {
		const result = await run(contributionClient(async () => ({ uri: 'at://x/c/1' })))

		expect(result.created).toBe(10)
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

describe('breakerTrips', () => {
	const limits = { ratio: 0.2, minFailures: 5, sameError: 10 }

	test('never trips below the minimum, even at 100%', () => {
		expect(breakerTrips(['a', 'b', 'c', 'd'], 0, limits)).toBeNull()
	})

	test('trips on the ratio of failures to write attempts', () => {
		expect(breakerTrips(['a', 'b', 'c', 'd', 'e'], 20, limits)).toMatch(/write attempts/)
		expect(breakerTrips(['a', 'b', 'c', 'd', 'e'], 30, limits)).toBeNull()
	})

	test('trips when enough failures share one error shape', () => {
		const msgs = Array.from({ length: 10 }, (_, i) => `script error for game ${i} at at://did:web:x/g/${i}`)
		expect(breakerTrips(msgs, 1000, limits)).toMatch(/share one error/)
	})
})

describe('errorShape', () => {
	test('drops ids, uris, dids and quoted values', () => {
		expect(errorShape(`Failed for "Portal 2": game 42 at at://did:web:g/x/1 by did:plc:abc`)).toBe(errorShape(`Failed for "Half-Life": game 7 at at://did:web:g/x/2 by did:plc:xyz`))
	})

	test('keeps different causes apart', () => {
		expect(errorShape('invalid releases')).not.toBe(errorShape('name too long'))
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
		[502, 'systemic'],
		[503, 'systemic'],
		[504, 'systemic'],
	])('status %i → %s', (status, kind) => {
		expect(classifyFailure(new ContributionRequestError('x', status, ''))).toBe(kind)
	})

	test('record-read failures: transport trouble is systemic, a bad record or DID is the game\'s', () => {
		expect(classifyFailure(new RecordUnreadableError('at://x/g/1', '602'))).toBe('entity')
		expect(classifyFailure(new RecordReadError('at://x/g/1', 400, 'InvalidRequest: Could not find repo'))).toBe('entity')
		expect(classifyFailure(new RecordReadError('at://x/g/1', 0, 'response exceeds the cap'))).toBe('entity')
		expect(classifyFailure(new RecordReadError('at://x/g/1', 503, 'Service Unavailable'))).toBe('systemic')
		expect(classifyFailure(new RecordReadError('at://x/g/1', 429, 'RateLimited'))).toBe('systemic')
		expect(classifyFailure(new PdsResolutionError('did:web:x', 'no atproto_pds service'))).toBe('entity')
		expect(classifyFailure(new PdsResolutionError('did:plc:x', 'returned 410'))).toBe('entity')
		expect(classifyFailure(new PdsResolutionError('did:web:x', 'fetch failed', true))).toBe('systemic')
		const timeout = new Error('The operation was aborted due to timeout'); timeout.name = 'TimeoutError'
		expect(classifyFailure(timeout)).toBe('systemic')
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

describe('syncEntityType with igdbIds', () => {
	let dbPath: string
	let state: StateManager

	beforeEach(() => {
		dbPath = join(tmpdir(), `sync-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		state = new StateManager(dbPath)
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

	/** An IGDB fake that records each paginate call and returns the ids its where clause names. */
	function recordingIgdb() {
		const calls: Array<{ startOffset: number; where: string }> = []
		const igdb = {
			async *paginate(_endpoint: string, _fields: string, startOffset: number, where: string) {
				calls.push({ startOffset, where })
				const ids = /^where id = \(([\d,]+)\);$/.exec(where)?.[1]?.split(',').map(Number) ?? [1, 2, 3]
				yield { items: ids.map((id) => ({ id })), offset: startOffset }
			},
		} as unknown as IGDBClient
		return { igdb, calls }
	}

	function run(igdb: IGDBClient, client: ContributionClient, extra: Partial<Parameters<typeof syncEntityType>[0]> = {}) {
		return syncEntityType<{ id: number }>({
			entityType: 'game',
			igdbEndpoint: 'games',
			igdbFields: 'fields name;',
			collection: 'games.gamesgamesgamesgames.game',
			contributionClient: client,
			mapRecord: async (item) => ({ name: `Game ${item.id}`, applicationType: 'game' }),
			...extra,
		} as Parameters<typeof syncEntityType<{ id: number }>>[0], igdb, {} as AtprotoClient, state)
	}

	const ok = () => contributionClient(async () => ({ uri: 'at://x/c/1' }))

	test('one where clause per page of 500 ids', () => {
		expect(IGDB_IDS_PAGE_SIZE).toBe(500)
		const ids = Array.from({ length: 1001 }, (_, i) => i + 1)
		const clauses = igdbIdWhereClauses(ids)
		expect(clauses).toHaveLength(3)
		expect(clauses[0]).toBe(`where id = (${ids.slice(0, 500).join(',')});`)
		expect(clauses[1]).toBe(`where id = (${ids.slice(500, 1000).join(',')});`)
		expect(clauses[2]).toBe('where id = (1001);')
		expect(igdbIdWhereClauses(ids.slice(0, 500))).toHaveLength(1)
		expect(igdbIdWhereClauses(ids.slice(0, 501))).toHaveLength(2)
		expect(igdbIdWhereClauses([])).toEqual([])
	})

	test('pages through every id, from offset 0, instead of updated_at', async () => {
		state.setLastSyncAt('game', PREVIOUS_CURSOR)
		const { igdb, calls } = recordingIgdb()
		const ids = Array.from({ length: 1200 }, (_, i) => 10_000 + i)
		const client = ok()
		const r = await run(igdb, client, { igdbIds: ids, startOffset: 42 })
		expect(calls.map((c) => c.where)).toEqual(igdbIdWhereClauses(ids))
		expect(calls.every((c) => c.startOffset === 0)).toBe(true)
		expect(calls.some((c) => c.where.includes('updated_at'))).toBe(false)
		expect(client.createContribution).toHaveBeenCalledTimes(1200)
		expect(r.created).toBe(1200)
	})

	test('never writes the cursor, with or without one, even on a clean run', async () => {
		const { igdb } = recordingIgdb()
		const r1 = await run(igdb, ok(), { igdbIds: [1, 2] })
		expect(r1.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('game')).toBeNull()

		state.setLastSyncAt('game', PREVIOUS_CURSOR)
		const r2 = await run(igdb, ok(), { igdbIds: [1, 2] })
		expect(r2.cursorAdvanced).toBe(false)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('charges no failed run to a streak, leaves the breaker count alone, and ignores a stale cursor', async () => {
		state.setLastSyncAt('game', PREVIOUS_CURSOR) // months old: the nightly run would alert
		state.setMeta('breaker_trips_game', '2')
		const { igdb } = recordingIgdb()
		const lua500 = contributionClient(async () => { throw new ContributionRequestError('createContribution', 500, 'script error') })
		const r = await run(igdb, lua500, { igdbIds: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] })
		expect(r.cursorAlert).toBe(false)
		expect(r.retryable).toBe(10)
		expect(r.systemic).toBe(1) // the breaker still reports it
		expect(state.getMetaNumber('breaker_trips_game')).toBe(2)
		expect(state.getFailureStreak('game', '1')).toBe(0)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR)
	})

	test('maps games already in state, which a full scan would skip', async () => {
		state.setEntity('game', '1', 'at://did:web:x/games.gamesgamesgamesgames.game/g1')
		const { igdb } = recordingIgdb()
		const mapRecord = vi.fn(async (item: { id: number }) => ({ name: `Game ${item.id}`, applicationType: 'game' }))
		await run(igdb, ok(), { igdbIds: [1], mapRecord })
		expect(mapRecord).toHaveBeenCalledTimes(1)
	})

	test('without igdbIds the run is unchanged: updated_at, startOffset, cursor', async () => {
		state.setLastSyncAt('game', PREVIOUS_CURSOR)
		vi.spyOn(Date, 'now').mockReturnValue((PREVIOUS_CURSOR + 86400) * 1000)
		const { igdb, calls } = recordingIgdb()
		const r = await run(igdb, ok(), { startOffset: 0 })
		expect(calls).toEqual([{ startOffset: 0, where: `where updated_at > ${PREVIOUS_CURSOR};` }])
		expect(r.cursorAdvanced).toBe(true)
		expect(state.getLastSyncAt('game')).toBe(PREVIOUS_CURSOR + 86400)
	})
})
