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
