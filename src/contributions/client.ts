import { HappyViewNodeClient } from '@happyview/oauth-client-node'
import { FileStorage } from './storage.js'
import { fetchWithRetry, type RetryOptions } from './retry.js'

/** A HappyView request that came back with a non-OK status. */
export class ContributionRequestError extends Error {
	constructor(
		readonly operation: string,
		readonly status: number,
		body: string,
	) {
		super(`${operation} returned ${status}: ${body}`)
		this.name = 'ContributionRequestError'
	}
}

/** The stored game record, without the read-time hydration getGame adds. */
export interface RawGameRecord {
	uri: string
	cid?: string
	value: Record<string, unknown>
}

export interface GameLookupResult {
	uri: string
	record: Record<string, unknown>
	redirectedFrom?: string
}

export interface ContributionResult {
	uri: string
}

export interface ContributionClientOptions {
	instanceUrl: string
	clientId: string
	clientKey: string
	clientSecret: string
	storagePath?: string
	retry?: RetryOptions
	/** Injectable for tests. Defaults to the global fetch. */
	fetch?: typeof fetch
}

export class ContributionClient {
	private hvClient: HappyViewNodeClient
	private session: Awaited<ReturnType<HappyViewNodeClient['restore']>> | null = null
	private instanceUrl: string
	private clientKey: string
	private retry: RetryOptions
	private fetchImpl: typeof fetch

	constructor(options: ContributionClientOptions) {
		this.instanceUrl = options.instanceUrl
		this.clientKey = options.clientKey
		this.retry = options.retry ?? {}
		this.fetchImpl = options.fetch ?? ((...args) => fetch(...args))
		this.hvClient = new HappyViewNodeClient({
			instanceUrl: options.instanceUrl,
			clientId: options.clientId,
			clientKey: options.clientKey,
			clientSecret: options.clientSecret,
			redirectUri: 'http://127.0.0.1:9473/oauth/callback',
			scopes: 'atproto transition:generic',
			storage: new FileStorage(options.storagePath),
		})
	}

	get oauthClient(): HappyViewNodeClient {
		return this.hvClient
	}

	async restore(did: string): Promise<void> {
		const session = await this.hvClient.restore(did)
		if (!session) {
			throw new Error(
				`No stored session for ${did}. Run "npm run setup-auth" to authenticate first.`,
			)
		}
		this.session = session
		console.log(`[contributions] Restored session for ${session.did}`)
	}

	async uploadBlob(data: Uint8Array, mimeType: string): Promise<{ ref: { $link: string }; mimeType: string; size: number }> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}

		const session = this.session
		const response = await fetchWithRetry('uploadBlob', () => session.fetchHandler(
			`${this.instanceUrl}/xrpc/com.atproto.repo.uploadBlob`,
			{
				method: 'POST',
				headers: { 'Content-Type': mimeType },
				body: Buffer.from(data),
			},
		), this.retry)

		if (!response.ok) {
			const errorBody = await response.text()
			throw new ContributionRequestError('uploadBlob', response.status, errorBody)
		}

		const result = await response.json() as { blob: { ref: { $link: string }; mimeType: string; size: number } }
		return {
			ref: result.blob.ref,
			mimeType: result.blob.mimeType,
			size: result.blob.size,
		}
	}

	async getGameByIgdbId(igdbId: string): Promise<GameLookupResult | null> {
		const url = `${this.instanceUrl}/xrpc/games.gamesgamesgamesgames.getGame?igdbId=${encodeURIComponent(igdbId)}`
		const response = await fetchWithRetry('getGame', () => this.fetchImpl(url, {
			headers: { 'X-Client-Key': this.clientKey },
		}), this.retry)

		if (response.status === 404 || response.status === 400) {
			return null
		}

		if (!response.ok) {
			const body = await response.text()
			throw new ContributionRequestError('getGame', response.status, body)
		}

		// getGame answers 200 with no `game` for an IGDB id it does not know.
		const data = await response.json() as { game?: (Record<string, unknown> & { uri?: string; redirectedFrom?: string }) | null }
		if (!data.game || typeof data.game.uri !== 'string') {
			return null
		}

		return {
			uri: data.game.uri,
			record: data.game as Record<string, unknown>,
			redirectedFrom: data.game.redirectedFrom,
		}
	}

	async getRecord(collection: string, rkey: string): Promise<Record<string, unknown> | null> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}

		const result = await this.fetchRecord(this.session.did, collection, rkey)
		return result as Record<string, unknown> | null
	}

	/**
	 * Fetch the stored game record behind a URI. getGame's response is hydrated
	 * for display (release platform names and so on), so diffing against it
	 * reports changes that are not there.
	 */
	async getGameRecord(uri: string): Promise<RawGameRecord | null> {
		const match = uri.match(/^at:\/\/([^/]+)\/([^/]+)\/([^/]+)$/)
		if (!match) {
			throw new Error(`not a record URI: ${uri}`)
		}
		const [, repo, collection, rkey] = match
		const result = await this.fetchRecord(repo, collection, rkey) as { uri?: string; cid?: string; value?: Record<string, unknown> } | null
		if (!result || !result.value) {
			return null
		}
		return { uri: result.uri ?? uri, cid: result.cid, value: result.value }
	}

	private async fetchRecord(repo: string, collection: string, rkey: string): Promise<unknown | null> {
		const url = `${this.instanceUrl}/xrpc/com.atproto.repo.getRecord?repo=${encodeURIComponent(repo)}&collection=${encodeURIComponent(collection)}&rkey=${encodeURIComponent(rkey)}`
		const response = await fetchWithRetry('getRecord', () => this.fetchImpl(url, {
			headers: { 'X-Client-Key': this.clientKey },
		}), this.retry)

		if (response.status === 404 || response.status === 400) {
			return null
		}

		if (!response.ok) {
			const body = await response.text()
			throw new ContributionRequestError('getRecord', response.status, body)
		}

		return await response.json()
	}

	/**
	 * Queue an auto-accept sweep of this account's own pending contributions.
	 * It retries media deferrals and jobs that failed, which nothing else does
	 * until HappyView can schedule jobs itself.
	 */
	async queueAutoAcceptSweep(limit = 5000): Promise<{ jobId: string; alreadyQueued: boolean; startedAt?: string }> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}
		const session = this.session
		const response = await fetchWithRetry('autoAcceptContributions', () => session.fetchHandler(
			`${this.instanceUrl}/xrpc/games.gamesgamesgamesgames.autoAcceptContributions`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ limit }),
			},
		), this.retry)
		if (!response.ok) {
			throw new ContributionRequestError('autoAcceptContributions', response.status, await response.text())
		}
		// HappyView answers with the sweep already pending or running, rather
		// than a second one, when there is one (a retried POST, or last night's
		// sweep still draining).
		const { jobId, alreadyQueued, startedAt } = await response.json() as { jobId: string; alreadyQueued?: boolean; startedAt?: string }
		return { jobId, alreadyQueued: alreadyQueued === true, ...(startedAt ? { startedAt } : {}) }
	}

	/**
	 * Whether an already-queued sweep has been running so long that its worker
	 * is probably stuck. A sweep chains jobs of at most 5000 rows each, so no
	 * single job should run for a day.
	 */
	static sweepLooksHung(result: { alreadyQueued: boolean; startedAt?: string }, now = Date.now(), maxHours = 24): boolean {
		if (!result.alreadyQueued || !result.startedAt) return false
		const started = Date.parse(result.startedAt)
		return Number.isFinite(started) && now - started > maxHours * 3600 * 1000
	}

	async createContribution(params: {
		contributionType: 'correction' | 'addition' | 'newGame'
		changes: Record<string, unknown>
		subject?: string
		rkey?: string
	}): Promise<ContributionResult> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}

		const body: Record<string, unknown> = {
			contributionType: params.contributionType,
			changes: params.changes,
		}
		if (params.subject) {
			body.subject = params.subject
		}
		if (params.rkey) {
			body.rkey = params.rkey
		}

		const session = this.session
		const response = await fetchWithRetry('createContribution', () => session.fetchHandler(
			`${this.instanceUrl}/xrpc/games.gamesgamesgamesgames.createContribution`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(body),
			},
		), this.retry)

		if (!response.ok) {
			const errorBody = await response.text()
			throw new ContributionRequestError('createContribution', response.status, errorBody)
		}

		return await response.json() as ContributionResult
	}
}
