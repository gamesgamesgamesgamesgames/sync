import { describe, test, expect, vi } from 'vitest'
import { ContributionClient, ContributionRequestError } from './client.js'

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
	return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } })
}

function client(fetchImpl: typeof fetch) {
	return new ContributionClient({
		instanceUrl: 'https://hv.test',
		clientId: 'https://client.test/metadata.json',
		clientKey: 'hvc_test',
		clientSecret: 'secret',
		storagePath: '/nonexistent',
		fetch: fetchImpl,
		retry: { sleep: async () => {} },
	})
}

describe('ContributionClient.getGameByIgdbId', () => {
	test('treats a 200 with no game as not found', async () => {
		const fetchImpl = vi.fn(async () => json(200, {}))
		expect(await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1')).toBeNull()
	})

	test('treats a 200 with game: null as not found', async () => {
		const fetchImpl = vi.fn(async () => json(200, { game: null }))
		expect(await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1')).toBeNull()
	})

	test('still treats 404 as not found', async () => {
		const fetchImpl = vi.fn(async () => json(404, { error: 'NotFound' }))
		expect(await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1')).toBeNull()
	})

	test('returns the game when there is one', async () => {
		const fetchImpl = vi.fn(async () => json(200, { game: { uri: 'at://did:web:x/games.gamesgamesgamesgames.game/a', name: 'Portal' } }))
		const game = await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1')
		expect(game?.uri).toBe('at://did:web:x/games.gamesgamesgamesgames.game/a')
	})

	test('retries a 429 instead of failing the game', async () => {
		const fetchImpl = vi.fn()
			.mockResolvedValueOnce(json(429, { error: 'RateLimited' }, { 'Retry-After': '1' }))
			.mockResolvedValueOnce(json(200, { game: { uri: 'at://did:web:x/games.gamesgamesgamesgames.game/a' } }))
		const game = await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1')
		expect(game?.uri).toBe('at://did:web:x/games.gamesgamesgamesgames.game/a')
		expect(fetchImpl).toHaveBeenCalledTimes(2)
	})

	test('throws a ContributionRequestError with the status on other errors', async () => {
		const fetchImpl = vi.fn(async () => json(500, { error: 'boom' }))
		const err = await client(fetchImpl as unknown as typeof fetch).getGameByIgdbId('1').catch((e) => e)
		expect(err).toBeInstanceOf(ContributionRequestError)
		expect(err.status).toBe(500)
	})
})

describe('ContributionClient.getGameRecord', () => {
	test('reads the stored record for the URI from getRecord', async () => {
		const fetchImpl = vi.fn(async (_url: string) => json(200, { uri: 'at://did:web:x/games.gamesgamesgamesgames.game/a', cid: 'bafy', value: { name: 'Portal' } }))
		const rec = await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x/games.gamesgamesgamesgames.game/a')
		expect(rec).toEqual({ uri: 'at://did:web:x/games.gamesgamesgamesgames.game/a', cid: 'bafy', value: { name: 'Portal' } })
		const url = new URL(fetchImpl.mock.calls[0][0])
		expect(url.pathname).toBe('/xrpc/com.atproto.repo.getRecord')
		expect(url.searchParams.get('repo')).toBe('did:web:x')
		expect(url.searchParams.get('collection')).toBe('games.gamesgamesgamesgames.game')
		expect(url.searchParams.get('rkey')).toBe('a')
	})

	test('returns null when the record is gone', async () => {
		const fetchImpl = vi.fn(async () => json(400, { error: 'RecordNotFound' }))
		expect(await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x/games.gamesgamesgamesgames.game/a')).toBeNull()
	})
})

describe('ContributionClient.queueAutoAcceptSweep', () => {
	function withSession(fetchHandler: (url: string, init: RequestInit) => Promise<Response>) {
		const c = client(vi.fn() as unknown as typeof fetch)
		;(c as unknown as { session: unknown }).session = { did: 'did:plc:sync', fetchHandler: vi.fn(fetchHandler) }
		return c
	}

	test('posts to autoAcceptContributions with the limit and returns the job id', async () => {
		let seen: { url: string; body: unknown } | undefined
		const c = withSession(async (url, init) => {
			seen = { url, body: JSON.parse(String(init.body)) }
			return json(200, { jobId: 'job-9' })
		})
		expect(await c.queueAutoAcceptSweep(1234)).toEqual({ jobId: 'job-9', alreadyQueued: false })
		expect(seen?.url).toBe('https://hv.test/xrpc/games.gamesgamesgamesgames.autoAcceptContributions')
		expect(seen?.body).toEqual({ limit: 1234 })
	})

	test('retries a 429', async () => {
		let n = 0
		const c = withSession(async () => (++n === 1 ? json(429, { error: 'RateLimited' }, { 'Retry-After': '1' }) : json(200, { jobId: 'job-2' })))
		expect((await c.queueAutoAcceptSweep()).jobId).toBe('job-2')
		expect(n).toBe(2)
	})

	test('reports a sweep that was already queued', async () => {
		const c = withSession(async () => json(200, { jobId: 'job-3', alreadyQueued: true }))
		expect(await c.queueAutoAcceptSweep()).toEqual({ jobId: 'job-3', alreadyQueued: true })
	})

	test('passes the existing sweep\'s start time through', async () => {
		const c = withSession(async () => json(200, { jobId: 'job-4', alreadyQueued: true, status: 'running', startedAt: '2026-11-14T00:00:00Z' }))
		expect(await c.queueAutoAcceptSweep()).toEqual({ jobId: 'job-4', alreadyQueued: true, startedAt: '2026-11-14T00:00:00Z' })
	})

	test('a sweep running for more than 24 hours looks hung', () => {
		const now = Date.parse('2026-11-15T01:00:00Z')
		expect(ContributionClient.sweepLooksHung({ alreadyQueued: true, startedAt: '2026-11-14T00:00:00Z' }, now)).toBe(true)
		expect(ContributionClient.sweepLooksHung({ alreadyQueued: true, startedAt: '2026-11-14T02:00:00Z' }, now)).toBe(false)
	})

	test('a pending, new or undated sweep never looks hung', () => {
		const now = Date.parse('2026-11-20T00:00:00Z')
		expect(ContributionClient.sweepLooksHung({ alreadyQueued: true }, now)).toBe(false)
		expect(ContributionClient.sweepLooksHung({ alreadyQueued: false, startedAt: '2026-11-01T00:00:00Z' }, now)).toBe(false)
		expect(ContributionClient.sweepLooksHung({ alreadyQueued: true, startedAt: 'not a date' }, now)).toBe(false)
	})

	test('a refusal throws with its status', async () => {
		const c = withSession(async () => json(500, { error: 'unauthorized' }))
		const err = await c.queueAutoAcceptSweep().catch((e) => e)
		expect(err).toBeInstanceOf(ContributionRequestError)
		expect(err.status).toBe(500)
	})

	test('needs a session', async () => {
		await expect(client(vi.fn() as unknown as typeof fetch).queueAutoAcceptSweep()).rejects.toThrow(/restore/)
	})
})
