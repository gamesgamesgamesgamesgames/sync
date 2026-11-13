import { describe, test, expect, vi } from 'vitest'
import { fetchWithRetry, retryDelayFromHeaders } from './retry.js'

function response(status: number, headers: Record<string, string> = {}, body = '{}'): Response {
	return new Response(body, { status, headers })
}

describe('retryDelayFromHeaders', () => {
	test('reads Retry-After seconds', () => {
		expect(retryDelayFromHeaders(new Headers({ 'Retry-After': '7' }))).toBe(7000)
	})

	test('reads a Retry-After date', () => {
		const now = Date.parse('2026-11-13T00:00:00Z')
		const headers = new Headers({ 'Retry-After': 'Fri, 13 Nov 2026 00:00:05 GMT' })
		expect(retryDelayFromHeaders(headers, now)).toBe(5000)
	})

	test('falls back to RateLimit-Reset', () => {
		expect(retryDelayFromHeaders(new Headers({ 'RateLimit-Reset': '3' }))).toBe(3000)
	})

	test('returns null with no hint', () => {
		expect(retryDelayFromHeaders(new Headers())).toBeNull()
	})
})

describe('fetchWithRetry', () => {
	test('retries a 429 after the Retry-After delay, then returns the success', async () => {
		const sleep = vi.fn(async (_ms: number) => {})
		const doFetch = vi.fn()
			.mockResolvedValueOnce(response(429, { 'Retry-After': '2' }))
			.mockResolvedValueOnce(response(429, { 'Retry-After': '4' }))
			.mockResolvedValueOnce(response(200))

		const res = await fetchWithRetry('getGame', doFetch, { sleep })

		expect(res.status).toBe(200)
		expect(doFetch).toHaveBeenCalledTimes(3)
		expect(sleep.mock.calls.map((c) => c[0])).toEqual([2000, 4000])
	})

	test('uses exponential backoff when no header is sent, capped by maxDelayMs', async () => {
		const sleep = vi.fn(async (_ms: number) => {})
		const doFetch = vi.fn()
			.mockResolvedValueOnce(response(503))
			.mockResolvedValueOnce(response(503))
			.mockResolvedValueOnce(response(503))
			.mockResolvedValueOnce(response(200))

		await fetchWithRetry('getRecord', doFetch, { sleep, baseDelayMs: 100, maxDelayMs: 300 })

		expect(sleep.mock.calls.map((c) => c[0])).toEqual([100, 200, 300])
	})

	test('gives up after maxRetries and returns the last 429', async () => {
		const sleep = vi.fn(async (_ms: number) => {})
		const doFetch = vi.fn(async () => response(429, { 'Retry-After': '1' }))

		const res = await fetchWithRetry('getGame', doFetch, { sleep, maxRetries: 2 })

		expect(res.status).toBe(429)
		expect(doFetch).toHaveBeenCalledTimes(3)
	})

	test('does not retry other client errors', async () => {
		const sleep = vi.fn(async (_ms: number) => {})
		const doFetch = vi.fn(async () => response(401))

		const res = await fetchWithRetry('createContribution', doFetch, { sleep })

		expect(res.status).toBe(401)
		expect(doFetch).toHaveBeenCalledTimes(1)
		expect(sleep).not.toHaveBeenCalled()
	})
})
