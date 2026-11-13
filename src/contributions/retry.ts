/**
 * Retry HappyView requests that were refused for being too fast or that hit a
 * transient server error.
 *
 * HappyView answers a rate-limited request with 429 and a `Retry-After` header
 * (seconds) plus `RateLimit-Reset`. Before this, the contribution client threw
 * on the first 429, so most games in a nightly run failed outright.
 */

export interface RetryOptions {
	/** How many times to retry after the first attempt. */
	maxRetries?: number
	/** Base delay for exponential backoff when the server gives no hint. */
	baseDelayMs?: number
	/** Upper bound for any single wait. */
	maxDelayMs?: number
	/** Injectable for tests. */
	sleep?: (ms: number) => Promise<void>
}

const RETRYABLE_STATUSES = new Set([429, 502, 503, 504])

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/** How long the server asked us to wait, in ms, or null when it did not say. */
export function retryDelayFromHeaders(headers: Headers, now = Date.now()): number | null {
	const retryAfter = headers.get('retry-after')
	if (retryAfter) {
		const seconds = Number(retryAfter)
		if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000
		const date = Date.parse(retryAfter)
		if (!Number.isNaN(date)) return Math.max(0, date - now)
	}

	const reset = headers.get('ratelimit-reset')
	if (reset) {
		const value = Number(reset)
		if (Number.isFinite(value) && value >= 0) {
			// Either seconds-until-reset or an epoch timestamp in seconds.
			return value > 1_000_000_000 ? Math.max(0, value * 1000 - now) : value * 1000
		}
	}

	return null
}

/**
 * Run `doFetch` until it returns a response that is not retryable, or retries
 * run out. The last response is returned either way, so the caller's own
 * status handling still applies.
 */
export async function fetchWithRetry(
	label: string,
	doFetch: () => Promise<Response>,
	options: RetryOptions = {},
): Promise<Response> {
	const maxRetries = options.maxRetries ?? 8
	const baseDelayMs = options.baseDelayMs ?? 1000
	const maxDelayMs = options.maxDelayMs ?? 120_000
	const sleep = options.sleep ?? defaultSleep

	for (let attempt = 0; ; attempt++) {
		const response = await doFetch()
		if (!RETRYABLE_STATUSES.has(response.status) || attempt >= maxRetries) {
			return response
		}

		const hinted = retryDelayFromHeaders(response.headers)
		const backoff = baseDelayMs * 2 ** attempt
		const delay = Math.min(maxDelayMs, hinted ?? backoff)

		// Drain the body so the connection can be reused.
		await response.text().catch(() => undefined)

		if (attempt === 0 || attempt === maxRetries - 1) {
			console.warn(`  [contributions] ${label} returned ${response.status}, retrying in ${Math.round(delay / 1000)}s (attempt ${attempt + 1}/${maxRetries})`)
		}
		await sleep(delay)
	}
}
