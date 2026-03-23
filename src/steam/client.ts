/**
 * Steam Store API client with rate limiting and retry logic.
 *
 * Two endpoints:
 * - appdetails: ~35 req/min (store.steampowered.com)
 * - GetItems: ~20 req/min (api.steampowered.com)
 */

import { countdownSleep } from '../helpers.js'

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

export interface SteamRequirements {
	minimum?: string
	recommended?: string
}

export interface SteamAppDetails {
	type: string
	name: string
	steam_appid: number
	is_free: boolean
	detailed_description: string
	about_the_game: string
	short_description: string
	supported_languages?: string
	header_image: string
	website: string | null
	pc_requirements: SteamRequirements | []
	mac_requirements: SteamRequirements | []
	linux_requirements: SteamRequirements | []
	developers?: string[]
	publishers?: string[]
	categories?: { id: number; description: string }[]
	genres?: { id: string; description: string }[]
	screenshots?: { id: number; path_thumbnail: string; path_full: string }[]
	movies?: {
		id: number
		name: string
		thumbnail: string
		webm: { [key: string]: string }
		mp4: { [key: string]: string }
		highlight: boolean
	}[]
	recommendations?: { total: number }
	achievements?: { total: number; highlighted: { name: string; path: string }[] }
	release_date?: { coming_soon: boolean; date: string }
	metacritic?: { score: number; url: string }
	content_descriptors?: { ids: number[]; notes: string | null }
	controller_support?: string
	dlc?: number[]
	price_overview?: {
		currency: string
		initial: number
		final: number
		discount_percent: number
		initial_formatted: string
		final_formatted: string
	}
}

export interface SteamStoreItem {
	appid: number
	name?: string
	type?: number
	[key: string]: unknown
}

// ---------------------------------------------------------------------------
// GetItems options
// ---------------------------------------------------------------------------

export interface GetItemsOptions {
	includeBasicInfo?: boolean
	includeFullDescription?: boolean
	includeScreenshots?: boolean
	includeTrailers?: boolean
	includeReviews?: boolean
	includeSupportedLanguages?: boolean
	includeReleaseDate?: boolean
	includePlatforms?: boolean
	includeTagCount?: boolean
	includeRatings?: boolean
	includeAllPurchaseOptions?: boolean
	includeAssets?: boolean
}

// ---------------------------------------------------------------------------
// Rate limiter (sliding window)
// ---------------------------------------------------------------------------

class RateLimiter {
	private maxRequests: number
	private windowMs: number
	private requestTimestamps: number[] = []

	constructor(maxRequests: number, windowMs: number) {
		this.maxRequests = maxRequests
		this.windowMs = windowMs
	}

	async wait(): Promise<void> {
		const now = Date.now()

		// Remove timestamps outside the window
		this.requestTimestamps = this.requestTimestamps.filter(
			(t) => now - t < this.windowMs,
		)

		if (this.requestTimestamps.length >= this.maxRequests) {
			const oldestInWindow = this.requestTimestamps[0]!
			const waitMs = this.windowMs - (now - oldestInWindow) + 50 // +50ms buffer
			if (waitMs > 0) {
				await countdownSleep(waitMs, '[steam] Rate limit')
			}
		}

		this.requestTimestamps.push(Date.now())
	}
}

// ---------------------------------------------------------------------------
// Fetch with retry
// ---------------------------------------------------------------------------

async function fetchWithRetry(
	url: string,
	init: RequestInit,
	maxRetries: number = 3,
): Promise<Response> {
	let lastError: Error | null = null

	for (let attempt = 0; attempt <= maxRetries; attempt++) {
		try {
			const response = await fetch(url, init)

			if (response.status === 429) {
				const retryAfter = response.headers.get('Retry-After')
				const waitMs = retryAfter
					? parseInt(retryAfter, 10) * 1000
					: 2000 * (attempt + 1)
				await countdownSleep(waitMs, '[steam] Rate limited')
				continue
			}

			if (response.status >= 500 && attempt < maxRetries) {
				const waitMs = 1000 * Math.pow(2, attempt)
				await countdownSleep(
					waitMs,
					`[steam] Server error (${response.status}), retrying`,
				)
				continue
			}

			return response
		} catch (err) {
			lastError = err as Error
			if (attempt < maxRetries) {
				const waitMs = 1000 * Math.pow(2, attempt)
				await countdownSleep(
					waitMs,
					`[steam] Request failed (${lastError.message}), retrying`,
				)
			}
		}
	}

	throw lastError ?? new Error('Fetch failed after retries')
}

// ---------------------------------------------------------------------------
// Steam client
// ---------------------------------------------------------------------------

export class SteamClient {
	private appDetailsLimiter = new RateLimiter(35, 60_000)
	private getItemsLimiter = new RateLimiter(20, 60_000)

	/**
	 * Fetch detailed app information from the Steam store.
	 * Returns null if the app is not found or the request is unsuccessful.
	 */
	async getAppDetails(appid: number): Promise<SteamAppDetails | null> {
		await this.appDetailsLimiter.wait()

		const url = `https://store.steampowered.com/api/appdetails?appids=${appid}`
		const response = await fetchWithRetry(url, {})

		if (!response.ok) {
			return null
		}

		const json = (await response.json()) as Record<
			string,
			{ success: boolean; data: SteamAppDetails }
		>

		const entry = json[String(appid)]
		if (!entry || !entry.success) {
			return null
		}

		return entry.data
	}

	/**
	 * Fetch store items in bulk via the IStoreBrowseService/GetItems endpoint.
	 */
	async getItems(
		appids: number[],
		options: GetItemsOptions = {},
	): Promise<SteamStoreItem[]> {
		await this.getItemsLimiter.wait()

		const {
			includeBasicInfo = true,
			includeFullDescription,
			includeScreenshots,
			includeTrailers,
			includeReviews,
			includeSupportedLanguages,
			includeReleaseDate,
			includePlatforms,
			includeTagCount,
			includeRatings,
			includeAllPurchaseOptions,
			includeAssets,
		} = options

		const dataRequest: Record<string, boolean> = {}
		if (includeBasicInfo) dataRequest.include_basic_info = true
		if (includeFullDescription) dataRequest.include_full_description = true
		if (includeScreenshots) dataRequest.include_screenshots = true
		if (includeTrailers) dataRequest.include_trailers = true
		if (includeReviews) dataRequest.include_reviews = true
		if (includeSupportedLanguages) dataRequest.include_supported_languages = true
		if (includeReleaseDate) dataRequest.include_release = true
		if (includePlatforms) dataRequest.include_platforms = true
		if (includeTagCount) dataRequest.include_tag_count = true
		if (includeRatings) dataRequest.include_ratings = true
		if (includeAllPurchaseOptions) dataRequest.include_all_purchase_options = true
		if (includeAssets) dataRequest.include_assets = true

		const body = {
			ids: appids.map((appid) => ({ appid })),
			context: {
				language: 'english',
				country_code: 'US',
				steam_realm: 1,
			},
			data_request: dataRequest,
		}

		const baseUrl =
			'https://api.steampowered.com/IStoreBrowseService/GetItems/v1/'
		const params = new URLSearchParams({
			input_json: JSON.stringify(body),
		})
		const url = `${baseUrl}?${params.toString()}`

		const response = await fetchWithRetry(url, {})

		if (!response.ok) {
			throw new Error(
				`Steam GetItems failed: ${response.status} ${await response.text()}`,
			)
		}

		const json = (await response.json()) as {
			response: { store_items?: SteamStoreItem[] }
		}

		return json.response.store_items ?? []
	}
}
