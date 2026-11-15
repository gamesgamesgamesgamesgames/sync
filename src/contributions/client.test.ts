import { describe, test, expect, vi } from 'vitest'
import { ContributionClient, ContributionRequestError, RecordReadError, RecordReadNotAllowedError } from './client.js'

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
	return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } })
}

function client(fetchImpl: typeof fetch, readableRepos: string[] = ['did:web:x.test']) {
	return new ContributionClient({
		readableRepos,
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
	test('asks for org credits only when told to', async () => {
		const fetchImpl = vi.fn(async (_url: string) => json(200, {}))
		const c = client(fetchImpl as unknown as typeof fetch)
		await c.getGameByIgdbId('7')
		await c.getGameByIgdbId('7', { includeOrgCredits: true })
		expect(fetchImpl.mock.calls[0]![0]).toBe('https://hv.test/xrpc/games.gamesgamesgamesgames.getGame?igdbId=7')
		expect(fetchImpl.mock.calls[1]![0]).toBe('https://hv.test/xrpc/games.gamesgamesgamesgames.getGame?igdbId=7&includeOrgCredits=true')
	})

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
	const DID_DOC = {
		id: 'did:web:x.test',
		service: [{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: 'https://pds.x.test' }],
	}

	/** Routes by host: the DID document, the repo's PDS, and HappyView (which must not be asked). */
	function routed(pds: (url: URL) => Response) {
		return vi.fn(async (input: string) => {
			const url = new URL(input)
			if (url.host === 'x.test' && url.pathname === '/.well-known/did.json') return json(200, DID_DOC)
			if (url.host === 'pds.x.test') return pds(url)
			if (url.host === 'hv.test' && url.pathname === '/xrpc/com.atproto.repo.getRecord') {
				// What HappyView answered on 2026-11-15 for every record.
				return json(400, { error: 'RecordNotFound', message: 'Could not locate record' })
			}
			throw new Error(`unexpected request to ${input}`)
		})
	}

	test('reads the stored record from the repo\'s own PDS, not from HappyView', async () => {
		const fetchImpl = routed((url) => {
			expect(url.pathname).toBe('/xrpc/com.atproto.repo.getRecord')
			expect(url.searchParams.get('repo')).toBe('did:web:x.test')
			expect(url.searchParams.get('collection')).toBe('games.gamesgamesgamesgames.game')
			expect(url.searchParams.get('rkey')).toBe('a')
			return json(200, { uri: 'at://did:web:x.test/games.gamesgamesgamesgames.game/a', cid: 'bafy', value: { name: 'Portal' } })
		})
		const rec = await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x.test/games.gamesgamesgamesgames.game/a')
		expect(rec).toEqual({ uri: 'at://did:web:x.test/games.gamesgamesgamesgames.game/a', cid: 'bafy', value: { name: 'Portal' } })
		expect(fetchImpl.mock.calls.some(([u]) => new URL(u as string).host === 'hv.test')).toBe(false)
	})

	test('returns null when the PDS says RecordNotFound', async () => {
		const fetchImpl = routed(() => json(400, { error: 'RecordNotFound', message: 'Could not locate record' }))
		expect(await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x.test/games.gamesgamesgamesgames.game/a')).toBeNull()
	})

	test('any other refusal throws a RecordReadError instead of passing for "not found"', async () => {
		const fetchImpl = routed(() => json(400, { error: 'InvalidRequest', message: 'Could not find repo' }))
		const err = await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x.test/games.gamesgamesgamesgames.game/a').catch((e) => e)
		expect(err).toBeInstanceOf(RecordReadError)
		expect(err.status).toBe(400)
	})

	test('a repo that is not allowlisted is refused before any request', async () => {
		const fetchImpl = vi.fn(async () => json(200, {}))
		const err = await client(fetchImpl as unknown as typeof fetch, []).getGameRecord('at://did:web:evil.test/games.gamesgamesgamesgames.game/a').catch((e) => e)
		expect(err).toBeInstanceOf(RecordReadNotAllowedError)
		expect(fetchImpl).not.toHaveBeenCalled()
	})

	test('reads refuse redirects and carry a timeout', async () => {
		const fetchImpl = routed(() => json(200, { uri: 'u', value: {} }))
		await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x.test/games.gamesgamesgamesgames.game/a')
		for (const [, init] of fetchImpl.mock.calls as unknown as Array<[string, RequestInit]>) {
			expect(init.redirect).toBe('error')
			expect(init.signal).toBeInstanceOf(AbortSignal)
		}
	})

	test('an oversized record response is refused', async () => {
		const big = JSON.stringify({ uri: 'u', value: { blob: 'x'.repeat(3 * 1024 * 1024) } })
		const fetchImpl = routed(() => new Response(big, { status: 200, headers: { 'content-type': 'application/json' } }))
		const err = await client(fetchImpl as unknown as typeof fetch).getGameRecord('at://did:web:x.test/games.gamesgamesgamesgames.game/a').catch((e) => e)
		expect(err).toBeInstanceOf(RecordReadError)
		expect(err.status).toBe(0)
	})

	test('hasBlob refuses a repo that is not allowlisted, before any request', async () => {
		const fetchImpl = vi.fn(async () => json(200, {}))
		const err = await client(fetchImpl as unknown as typeof fetch, []).hasBlob('did:web:evil.test', 'bafy').catch((e) => e)
		expect(err).toBeInstanceOf(RecordReadNotAllowedError)
		expect(fetchImpl).not.toHaveBeenCalled()
	})

	test('hasBlob asks the catalogue PDS for an allowlisted catalogue DID', async () => {
		const fetchImpl = routed((url) => {
			expect(url.pathname).toBe('/xrpc/com.atproto.sync.getBlob')
			expect(url.searchParams.get('did')).toBe('did:web:x.test')
			return url.searchParams.get('cid') === 'bafyHere' ? new Response('img', { status: 200 }) : json(400, { error: 'BlobNotFound' })
		})
		const c = client(fetchImpl as unknown as typeof fetch)
		expect(await c.hasBlob('did:web:x.test', 'bafyHere')).toBe(true)
		expect(await c.hasBlob('did:web:x.test', 'bafyGone')).toBe(false)
	})

	test('the own-contribution dedupe reads from the sync account\'s PDS too', async () => {
		const fetchImpl = vi.fn(async (input: string) => {
			const url = new URL(input)
			if (url.host === 'plc.directory') {
				return json(200, { id: 'did:plc:sync', service: [{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: 'https://pds.sync.test' }] })
			}
			if (url.host === 'pds.sync.test') return json(200, { uri: 'at://did:plc:sync/games.gamesgamesgamesgames.contribution/h', value: {} })
			throw new Error(`unexpected request to ${input}`)
		})
		const c = client(fetchImpl as unknown as typeof fetch)
		;(c as unknown as { session: unknown }).session = { did: 'did:plc:sync', fetchHandler: vi.fn() }
		expect(await c.getRecord('games.gamesgamesgamesgames.contribution', 'h')).not.toBeNull()
		expect(fetchImpl.mock.calls.map(([u]) => new URL(u as string).host)).toEqual(['plc.directory', 'pds.sync.test'])
	})
})

describe('ContributionClient.queueMaintenanceJob', () => {
	function withSession(fetchHandler: (url: string, init: RequestInit) => Promise<Response>) {
		const c = client(vi.fn() as unknown as typeof fetch)
		;(c as unknown as { session: unknown }).session = { did: 'did:plc:sync', fetchHandler: vi.fn(fetchHandler) }
		return c
	}

	test('posts the input to the procedure and returns the job id', async () => {
		let seen: { url: string; body: unknown } | undefined
		const c = withSession(async (url, init) => {
			seen = { url, body: JSON.parse(String(init.body)) }
			return json(200, { jobId: 'job-7' })
		})
		expect(await c.queueMaintenanceJob('games.gamesgamesgamesgames.syncWikidata', { dryRun: true })).toEqual({ jobId: 'job-7', alreadyQueued: false })
		expect(seen?.url).toBe('https://hv.test/xrpc/games.gamesgamesgamesgames.syncWikidata')
		expect(seen?.body).toEqual({ dryRun: true })
	})

	test('reports a job that was already pending or running', async () => {
		const c = withSession(async () => json(200, { jobId: 'job-1', alreadyQueued: true, status: 'running' }))
		expect(await c.queueMaintenanceJob('games.gamesgamesgamesgames.refreshPublisherCounts', {})).toEqual({ jobId: 'job-1', alreadyQueued: true, status: 'running' })
	})

	test('throws on a refusal, naming the procedure', async () => {
		const c = withSession(async () => json(400, { error: 'InvalidRequest', message: 'unauthorized' }))
		await expect(c.queueMaintenanceJob('games.gamesgamesgamesgames.reindexSearchGames', {})).rejects.toThrow(/reindexSearchGames returned 400/)
	})

	test('needs a restored session', async () => {
		const c = client(vi.fn() as unknown as typeof fetch)
		await expect(c.queueMaintenanceJob('games.gamesgamesgamesgames.syncWikidata', {})).rejects.toThrow(/restore/)
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
