/**
 * IGDB API client with Twitch OAuth authentication and rate limiting.
 *
 * Rate limit: 4 requests per second.
 * Pagination: max 500 items per request via offset.
 */

import { countdownSleep } from '../helpers.js'

const IGDB_BASE_URL = 'https://api.igdb.com/v4'
const TWITCH_TOKEN_URL = 'https://id.twitch.tv/oauth2/token'
const MAX_REQUESTS_PER_SECOND = 4
const PAGE_SIZE = 500

interface TwitchToken {
	access_token: string
	expires_in: number
	token_type: string
}

export class IGDBClient {
	private clientId: string
	private clientSecret: string
	private accessToken: string | null = null
	private tokenExpiresAt: number = 0
	private requestTimestamps: number[] = []
	private retry: IGDBRetryOptions

	constructor(clientId: string, clientSecret: string, retry: IGDBRetryOptions = {}) {
		this.clientId = clientId
		this.clientSecret = clientSecret
		this.retry = retry
	}

	/** Authenticate with Twitch to get an IGDB access token. */
	async authenticate(): Promise<void> {
		const params = new URLSearchParams({
			client_id: this.clientId,
			client_secret: this.clientSecret,
			grant_type: 'client_credentials',
		})

		const response = await fetch(TWITCH_TOKEN_URL, {
			method: 'POST',
			body: params,
		})

		if (!response.ok) {
			throw new Error(`Twitch auth failed: ${response.status} ${await response.text()}`)
		}

		const data = (await response.json()) as TwitchToken
		this.accessToken = data.access_token
		// Refresh 5 minutes before actual expiry
		this.tokenExpiresAt = Date.now() + (data.expires_in - 300) * 1000

		console.log('[igdb] Authenticated with Twitch')
	}

	/** Ensure we have a valid access token. */
	private async ensureAuth(): Promise<void> {
		if (!this.accessToken || Date.now() >= this.tokenExpiresAt) {
			await this.authenticate()
		}
	}

	/** Enforce rate limit of 4 requests/second using a sliding window. */
	private async rateLimit(): Promise<void> {
		const now = Date.now()

		// Remove timestamps older than 1 second
		this.requestTimestamps = this.requestTimestamps.filter((t) => now - t < 1000)

		if (this.requestTimestamps.length >= MAX_REQUESTS_PER_SECOND) {
			// Wait until the oldest request in the window is > 1s old
			const oldestInWindow = this.requestTimestamps[0]!
			const waitMs = 1000 - (now - oldestInWindow) + 10 // +10ms buffer
			if (waitMs > 0) {
				await sleep(waitMs)
			}
		}

		this.requestTimestamps.push(Date.now())
	}

	/** Make a rate-limited, authenticated POST request to an IGDB endpoint. */
	async query<T>(endpoint: string, body: string): Promise<T[]> {
		await this.ensureAuth()
		await this.rateLimit()

		const url = `${IGDB_BASE_URL}/${endpoint}`

		const response = await fetchWithRetry(url, {
			method: 'POST',
			headers: {
				'Client-ID': this.clientId,
				Authorization: `Bearer ${this.accessToken}`,
				Accept: 'application/json',
			},
			body,
		}, this.retry)

		if (response.status === 401) {
			// Token expired — re-authenticate and retry once
			await this.authenticate()
			await this.rateLimit()
			const retryResponse = await fetch(url, {
				method: 'POST',
				headers: {
					'Client-ID': this.clientId,
					Authorization: `Bearer ${this.accessToken}`,
					Accept: 'application/json',
				},
				body,
			})
			if (!retryResponse.ok) {
				throw new Error(`IGDB ${endpoint} failed after re-auth: ${retryResponse.status}`)
			}
			return (await retryResponse.json()) as T[]
		}

		if (!response.ok) {
			const text = await response.text()
			throw new Error(`IGDB ${endpoint} failed: ${response.status} ${text}`)
		}

		return (await response.json()) as T[]
	}

	/**
	 * Paginate through an IGDB endpoint, yielding batches of results.
	 *
	 * @param endpoint - The IGDB endpoint (e.g. "games", "platforms")
	 * @param fields - The fields clause (e.g. "fields name, slug;")
	 * @param startOffset - Offset to resume from
	 * @param where - Optional where clause (e.g. "where id > 100;")
	 */
	async *paginate<T>(
		endpoint: string,
		fields: string,
		startOffset: number = 0,
		where: string = '',
	): AsyncGenerator<{ items: T[]; offset: number }> {
		let offset = startOffset

		while (true) {
			const body = `${fields} ${where} limit ${PAGE_SIZE}; offset ${offset}; sort id asc;`
			const items = await this.query<T>(endpoint, body)

			if (items.length === 0) {
				break
			}

			yield { items, offset }
			offset += items.length

			if (items.length < PAGE_SIZE) {
				break
			}
		}
	}

	/** Download an image from IGDB's image CDN (no rate limit — CDN has no per-client cap). */
	async downloadImage(imageId: string, size: string = 'screenshot_huge'): Promise<Buffer> {
		const url = `https://images.igdb.com/igdb/image/upload/t_${size}/${imageId}.jpg`
		const response = await fetchWithRetry(url, {}, this.retry)

		if (!response.ok) {
			throw new Error(`Image download failed for ${imageId}: ${response.status}`)
		}

		const arrayBuffer = await response.arrayBuffer()
		return Buffer.from(arrayBuffer)
	}
}

/** Sleep for a given number of milliseconds. */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

export interface IGDBRetryOptions {
	/** Retries after the first attempt (default 6). */
	maxRetries?: number
	/** First backoff wait; doubles each retry (default 2 s). */
	baseDelayMs?: number
	/** Upper bound on any single wait, Retry-After included (default 60 s). */
	maxDelayMs?: number
	/** Injectable for tests. */
	fetch?: typeof fetch
	/** Injectable for tests. */
	sleep?: (ms: number, label: string) => Promise<void>
}

/**
 * Fetch with bounded retries for 429, 5xx and network errors.
 *
 * IGDB sits behind an API gateway that answers 504 "Endpoint request timed
 * out" when a query is slow. Those pass after a short wait, but the old three
 * quick retries (7 s in all) were not enough, and a single 504 ended the
 * nightly run. Waits double from `baseDelayMs` and never exceed
 * `maxDelayMs`, so with the defaults a request gives up after about two
 * minutes. When retries run out, the last response is returned, so callers
 * still report the real status.
 */
export async function fetchWithRetry(
	url: string,
	init: RequestInit,
	options: IGDBRetryOptions = {},
): Promise<Response> {
	const maxRetries = options.maxRetries ?? 6
	const baseDelayMs = options.baseDelayMs ?? 2000
	const maxDelayMs = options.maxDelayMs ?? 60_000
	const doFetch = options.fetch ?? fetch
	const wait = options.sleep ?? countdownSleep
	const backoff = (attempt: number) => Math.min(maxDelayMs, baseDelayMs * 2 ** attempt)

	let lastError: Error | null = null

	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		const isLast = attempt === maxRetries
		try {
			const response = await doFetch(url, init)

			if (response.status === 429 && !isLast) {
				const retryAfter = Number(response.headers.get('Retry-After'))
				const waitMs = Number.isFinite(retryAfter) && retryAfter > 0
					? Math.min(maxDelayMs, retryAfter * 1000)
					: backoff(attempt)
				await response.text().catch(() => undefined)
				await wait(waitMs, '[igdb] Rate limited')
				continue
			}

			if (response.status >= 500 && !isLast) {
				await response.text().catch(() => undefined)
				await wait(backoff(attempt), `[igdb] Server error (${response.status}), retrying`)
				continue
			}

			return response
		} catch (err) {
			lastError = err as Error
			if (!isLast) {
				await wait(backoff(attempt), `[igdb] Request failed (${lastError.message}), retrying`)
			}
		}
	}

	throw lastError ?? new Error('Fetch failed after retries')
}
