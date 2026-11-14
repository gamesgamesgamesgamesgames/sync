import { describe, test, expect, vi } from 'vitest'
import { IGDBClient, fetchWithRetry } from './client.js'

const res = (status: number, body = '[]', headers: Record<string, string> = {}) => new Response(body, { status, headers })

function sequence(...responses: Array<Response | Error>) {
	const queue = [...responses]
	return vi.fn(async () => {
		const next = queue.shift()
		if (!next) throw new Error('no more responses')
		if (next instanceof Error) throw next
		return next
	})
}

describe('fetchWithRetry (IGDB)', () => {
	test('retries a 504 and returns the success', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = sequence(res(504, '{"message": "Endpoint request timed out"}'), res(504), res(200, '[{"id":1}]'))
		const r = await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep })
		expect(r.status).toBe(200)
		expect(doFetch).toHaveBeenCalledTimes(3)
		expect(sleep.mock.calls.map((c) => (c as unknown[])[0])).toEqual([2000, 4000])
	})

	test('backoff doubles and is capped by maxDelayMs', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = sequence(res(502), res(503), res(504), res(500), res(200))
		await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep, baseDelayMs: 1000, maxDelayMs: 5000 })
		expect(sleep.mock.calls.map((c) => (c as unknown[])[0])).toEqual([1000, 2000, 4000, 5000])
	})

	test('honours Retry-After on a 429, capped by maxDelayMs', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = sequence(res(429, '', { 'Retry-After': '3' }), res(429, '', { 'Retry-After': '9999' }), res(200))
		await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep, maxDelayMs: 60_000 })
		expect(sleep.mock.calls.map((c) => (c as unknown[])[0])).toEqual([3000, 60_000])
	})

	test('gives up after maxRetries and returns the last response', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = vi.fn(async () => res(504))
		const r = await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep, maxRetries: 2 })
		expect(r.status).toBe(504)
		expect(doFetch).toHaveBeenCalledTimes(3)
		expect(sleep).toHaveBeenCalledTimes(2)
	})

	test('a 429 on the last attempt is returned, not thrown away', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = vi.fn(async () => res(429))
		const r = await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep, maxRetries: 1 })
		expect(r.status).toBe(429)
	})

	test('retries network errors, then rethrows the last one', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = sequence(new TypeError('fetch failed'), new TypeError('fetch failed'))
		await expect(fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep, maxRetries: 1 })).rejects.toThrow('fetch failed')
		expect(sleep).toHaveBeenCalledTimes(1)
	})

	test('does not retry a 400', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = vi.fn(async () => res(400))
		const r = await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep })
		expect(r.status).toBe(400)
		expect(doFetch).toHaveBeenCalledTimes(1)
		expect(sleep).not.toHaveBeenCalled()
	})

	test('the default worst case stays bounded (about two minutes)', async () => {
		const sleep = vi.fn(async () => {})
		const doFetch = vi.fn(async () => res(504))
		await fetchWithRetry('u', {}, { fetch: doFetch as unknown as typeof fetch, sleep })
		const total = sleep.mock.calls.reduce((n, c) => n + ((c as unknown[])[0] as number), 0)
		expect(doFetch).toHaveBeenCalledTimes(7)
		expect(total).toBe(2000 + 4000 + 8000 + 16000 + 32000 + 60000)
	})
})

describe('IGDBClient.query', () => {
	function client(doFetch: ReturnType<typeof vi.fn>) {
		const c = new IGDBClient('id', 'secret', { fetch: doFetch as unknown as typeof fetch, sleep: async () => {} })
		Object.assign(c as unknown as Record<string, unknown>, { accessToken: 'token', tokenExpiresAt: Date.now() + 3_600_000 })
		return c
	}

	test('a 504 from IGDB no longer ends the run', async () => {
		const doFetch = sequence(res(504, '{"message": "Endpoint request timed out"}'), res(200, '[{"id":7}]'))
		await expect(client(doFetch).query('games', 'fields id;')).resolves.toEqual([{ id: 7 }])
	})

	test('a 504 that outlasts the retries still throws with the status', async () => {
		const doFetch = vi.fn(async () => res(504, '{"message": "Endpoint request timed out"}'))
		await expect(client(doFetch).query('games', 'fields id;')).rejects.toThrow('IGDB games failed: 504')
	})
})
