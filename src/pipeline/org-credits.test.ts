import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StateManager } from '../state.js'
import { ContributionRequestError } from '../contributions/client.js'
import type { IGDBInvolvedCompany } from '../igdb/types.js'
import { creditRkey, syncOrgCredits, type OrgCreditsOptions } from './org-credits.js'

const CATALOGUE = 'did:web:catalogue.test'
const GAME = (n: number) => `at://${CATALOGUE}/games.gamesgamesgamesgames.game/g${n}`
const ORG = (n: number) => `at://${CATALOGUE}/games.gamesgamesgamesgames.org.profile/o${n}`
const COLLECTION = 'games.gamesgamesgamesgames.org.credit'

const dev = (icId: number, companyId: number, name = `Studio ${companyId}`): IGDBInvolvedCompany =>
	({ id: icId, company: { id: companyId, name }, developer: true })

const OPTIONS: OrgCreditsOptions = { maxGames: 100, maxWrites: 100, maxAttempts: 3, breaker: { ratio: 0.2, minFailures: 5, sameError: 10 } }

describe('syncOrgCredits', () => {
	let dbPath: string
	let state: StateManager

	beforeEach(() => {
		dbPath = join(tmpdir(), `credits-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
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

	/** Games 1..n exist in the catalogue; orgs listed have profiles. */
	function world(opts: { games?: number[]; credits?: Record<number, Array<{ orgUri?: string; displayName?: string; uri?: string }>>; gameOwner?: string } = {}) {
		const games = new Set(opts.games ?? [1, 2, 3])
		const igdb = { query: vi.fn(async (_endpoint: string, _body: string): Promise<unknown[]> => []) }
		const atproto = {
			getDid: () => CATALOGUE,
			applyCreates: vi.fn(async (writes: Array<{ collection: string; rkey?: string; record: Record<string, unknown> }>) =>
				writes.map((w) => ({ uri: `at://${CATALOGUE}/${w.collection}/${w.rkey}`, cid: 'bafycredit' }))),
		}
		const contributions = {
			getGameByIgdbId: vi.fn(async (id: string) => games.has(Number(id))
				? { uri: opts.gameOwner ? `at://${opts.gameOwner}/games.gamesgamesgamesgames.game/g${id}` : GAME(Number(id)), record: { orgCredits: opts.credits?.[Number(id)] } }
				: null),
			getGameRecord: vi.fn(async (uri: string) => ({ uri, cid: uri.includes('org.profile') ? `bafyorg-${uri.slice(-2)}` : `bafygame-${uri.slice(-2)}`, value: {} })),
		}
		return { igdb, atproto, contributions }
	}

	const run = (w: ReturnType<typeof world>, options: Partial<OrgCreditsOptions> = {}) =>
		syncOrgCredits(w.igdb as never, w.atproto as never, w.contributions as never, state, { ...OPTIONS, ...options })

	test('writes a credit with real game and org strongRefs at a deterministic rkey, into the catalogue repo', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(1, [dev(100, 10, 'Grezzo')])
		const w = world()
		const r = await run(w)
		expect(r.written).toBe(1)
		expect(w.atproto.applyCreates).toHaveBeenCalledTimes(1)
		const [write] = w.atproto.applyCreates.mock.calls[0]![0] as Array<{ collection: string; rkey: string; record: Record<string, unknown> }>
		expect(write!.collection).toBe(COLLECTION)
		expect(write!.rkey).toBe(creditRkey(GAME(1), 10))
		expect(write!.record).toMatchObject({
			org: { uri: ORG(10), cid: 'bafyorg-10' },
			game: { uri: GAME(1), cid: 'bafygame-g1' },
			roles: ['developer'],
			displayName: 'Grezzo',
		})
		expect(w.contributions.getGameByIgdbId).toHaveBeenCalledWith('1', { includeOrgCredits: true })
		expect(state.hasEntity('orgCredit', 100)).toBe(true)
		expect(state.getCreditQueueSize()).toBe(0)
	})

	test('the rkey depends only on the game and company', () => {
		expect(creditRkey(GAME(1), 10)).toBe(creditRkey(GAME(1), 10))
		expect(creditRkey(GAME(1), 10)).not.toBe(creditRkey(GAME(1), 11))
		expect(creditRkey(GAME(1), 10)).not.toBe(creditRkey(GAME(2), 10))
		expect(creditRkey(GAME(1), 10)).toMatch(/^[0-9a-f]{32}$/)
	})

	test('skips credits already in state, and ones getGame lists by org URI or display name', async () => {
		state.setEntity('org', 10, ORG(10))
		state.setEntity('org', 11, ORG(11))
		state.setEntity('org', 12, ORG(12))
		state.setEntity('orgCredit', 100, 'at://x/c/old')
		state.enqueueCredits(1, [dev(100, 10), dev(101, 11), dev(102, 12, 'Nintendo'), dev(103, 13)])
		state.setEntity('org', 13, ORG(13))
		const w = world({ credits: { 1: [{ orgUri: ORG(11), uri: 'at://x/c/11' }, { displayName: 'Nintendo', uri: 'at://x/c/12' }] } })
		const r = await run(w)
		expect(r.existing).toBe(3)
		expect(r.written).toBe(1)
		const writes = w.atproto.applyCreates.mock.calls[0]![0] as unknown as Array<{ record: { org: { uri: string } } }>
		expect(writes.map((x) => x.record.org.uri)).toEqual([ORG(13)])
		expect(state.getEntity('orgCredit', 101)).toBe('at://x/c/11')
		expect(state.getEntity('orgCredit', 102)).toBe('at://x/c/12')
	})

	test('a second run writes nothing', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(1, [dev(100, 10)])
		const w = world()
		await run(w)
		state.enqueueCredits(1, [dev(100, 10)]) // the game changed again on IGDB
		const r = await run(w)
		expect(r.written).toBe(0)
		expect(w.atproto.applyCreates).toHaveBeenCalledTimes(1)
	})

	test('a game with no catalogue game yet stays queued, then is dropped after maxAttempts', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(9, [dev(100, 10)])
		const w = world({ games: [] })
		for (let i = 1; i < OPTIONS.maxAttempts; i++) {
			const r = await run(w)
			expect(r.waiting).toBe(1)
			expect(state.getCreditQueueSize()).toBe(1)
		}
		const r = await run(w)
		expect(r.dropped).toBe(1)
		expect(state.getCreditQueueSize()).toBe(0)
		expect(w.atproto.applyCreates).not.toHaveBeenCalled()
	})

	test('a company without an org profile waits; the others are written', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(1, [dev(100, 10), dev(101, 11)])
		const w = world()
		const r = await run(w)
		expect(r.written).toBe(1)
		expect(r.waiting).toBe(1)
		expect(state.getCreditQueueSize()).toBe(1)
		// The profile shows up (the company sync created it): the next run finishes the game.
		state.setEntity('org', 11, ORG(11))
		const r2 = await run(w)
		expect(r2.written).toBe(1)
		expect(state.getCreditQueueSize()).toBe(0)
	})

	test('a game in someone else\'s repo is not credited', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(1, [dev(100, 10)])
		const w = world({ gameOwner: 'did:plc:studio' })
		const r = await run(w)
		expect(r.ownerHeld).toBe(1)
		expect(w.atproto.applyCreates).not.toHaveBeenCalled()
		expect(state.getCreditQueueSize()).toBe(0)
	})

	test('stops at maxWrites and leaves the rest queued', async () => {
		for (let g = 1; g <= 3; g++) {
			state.setEntity('org', 10 + g, ORG(10 + g))
			state.enqueueCredits(g, [dev(100 + g, 10 + g)], 1000 + g)
		}
		const w = world()
		const r = await run(w, { maxWrites: 2 })
		expect(r.written).toBe(2)
		expect(state.getCreditQueueSize()).toBe(1)
		expect(state.getCreditQueue(10).map((q) => q.igdbId)).toEqual([3])
	})

	test('a backfilled game fetches its companies from IGDB, 500 ids per query', async () => {
		state.setEntity('org', 10, ORG(10))
		for (let id = 1; id <= 501; id++) state.enqueueCredits(id, null)
		const w = world({ games: [1] })
		w.igdb.query.mockImplementation(async (_endpoint: string, body: string) =>
			body.includes('where id = (1,') ? [{ id: 1, involved_companies: [dev(100, 10)] }] : [])
		const r = await run(w, { maxGames: 1000 })
		expect(w.igdb.query).toHaveBeenCalledTimes(2)
		expect(w.igdb.query.mock.calls[0]![0]).toBe('games')
		expect(w.igdb.query.mock.calls[0]![1]).toContain('involved_companies.company.id')
		expect(r.written).toBe(1)
		// IGDB had no companies for the rest, so nothing is left to do for them.
		expect(state.getCreditQueueSize()).toBe(0)
	})

	test('a backfill never overwrites companies the game pass queued', () => {
		state.enqueueCredits(1, [dev(100, 10)])
		state.enqueueCredits(1, null)
		expect(state.getCreditQueue(1)[0]!.involved).toEqual([dev(100, 10)])
	})

	test('a systemic failure stops the step and keeps everything queued', async () => {
		state.setEntity('org', 10, ORG(10))
		state.enqueueCredits(1, [dev(100, 10)])
		state.enqueueCredits(2, [dev(101, 10)])
		const w = world()
		w.atproto.applyCreates.mockRejectedValue(new ContributionRequestError('applyWrites', 503, 'unavailable'))
		const r = await run(w)
		expect(r.aborted).toMatch(/systemic/)
		expect(r.systemic).toBe(1)
		expect(w.atproto.applyCreates).toHaveBeenCalledTimes(1)
		expect(state.getCreditQueueSize()).toBe(2)
	})

	test('the breaker stops the step when per-game failures pile up', async () => {
		state.setEntity('org', 10, ORG(10))
		for (let g = 1; g <= 20; g++) state.enqueueCredits(g, [dev(100 + g, 10)], 1000 + g)
		const w = world({ games: Array.from({ length: 20 }, (_, i) => i + 1) })
		w.atproto.applyCreates.mockRejectedValue(new Error('script error'))
		const r = await run(w)
		expect(r.aborted).toBeDefined()
		expect(r.failed).toBe(OPTIONS.breaker.minFailures)
		expect(state.getCreditQueueSize()).toBe(20)
	})
})
