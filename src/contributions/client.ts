import { HappyViewNodeClient } from '@happyview/oauth-client-node'
import { FileStorage } from './storage.js'
import { fetchWithRetry, type RetryOptions } from './retry.js'
import { PdsResolver, SAFE_READ_TIMEOUT_MS, readCapped } from '../atproto/pds-resolver.js'

const MAX_RECORD_BYTES = 2 * 1024 * 1024

/**
 * Reading a record from its repo's PDS failed for a reason other than "not
 * found". `status` is the HTTP status, or 0 when the response itself was
 * refused (too large, not JSON).
 */
export class RecordReadError extends Error {
	constructor(readonly uri: string, readonly status: number, body: string) {
		super(`getRecord for ${uri} returned ${status}: ${body.slice(0, 300)}`)
		this.name = 'RecordReadError'
	}
}

/** The repo is not one this client may read raw records from. */
export class RecordReadNotAllowedError extends Error {
	constructor(readonly repo: string) {
		super(`raw reads from ${repo} are not allowed`)
		this.name = 'RecordReadNotAllowedError'
	}
}

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
	/** Finds the PDS a repo lives on. Injectable for tests. */
	pdsResolver?: PdsResolver
	/**
	 * Repos (DIDs) whose records may be read straight from their PDS. The
	 * session's own DID is always allowed once restored. Anything else is
	 * refused, so a DID document can't point these reads at arbitrary hosts.
	 */
	readableRepos?: string[]
}

export class ContributionClient {
	private hvClient: HappyViewNodeClient
	private session: Awaited<ReturnType<HappyViewNodeClient['restore']>> | null = null
	private instanceUrl: string
	private clientKey: string
	private retry: RetryOptions
	private fetchImpl: typeof fetch
	private pdsResolver: PdsResolver
	private readableRepos: Set<string>

	constructor(options: ContributionClientOptions) {
		this.instanceUrl = options.instanceUrl
		this.clientKey = options.clientKey
		this.retry = options.retry ?? {}
		this.fetchImpl = options.fetch ?? ((...args) => fetch(...args))
		this.pdsResolver = options.pdsResolver ?? new PdsResolver({ fetch: this.fetchImpl })
		this.readableRepos = new Set((options.readableRepos ?? []).filter(Boolean))
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
	 * Whether an allowlisted repo's PDS holds a blob, through the public
	 * com.atproto.sync.getBlob. The body is never read. Throws on anything but
	 * 200 or a not-found answer.
	 */
	async hasBlob(did: string, cid: string): Promise<boolean> {
		if (!this.canReadRepo(did)) {
			throw new RecordReadNotAllowedError(did)
		}
		const pds = await this.pdsResolver.resolve(did)
		const url = `${pds}/xrpc/com.atproto.sync.getBlob?did=${encodeURIComponent(did)}&cid=${encodeURIComponent(cid)}`
		const response = await fetchWithRetry('getBlob', () => this.fetchImpl(url, {
			redirect: 'error',
			signal: AbortSignal.timeout(SAFE_READ_TIMEOUT_MS),
		}), this.retry)
		const status = response.status
		await response.body?.cancel().catch(() => undefined)
		if (status === 200) return true
		if (status === 404 || status === 400) return false
		throw new RecordReadError(`at://${did}/blob/${cid}`, status, 'getBlob')
	}

	/** Whether the sync account's own PDS already holds a blob. */
	async hasContributorBlob(cid: string): Promise<boolean> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}
		return this.hasBlob(this.session.did, cid)
	}

	/** Whether raw records for this repo may be read from its PDS. */
	canReadRepo(repo: string): boolean {
		return this.readableRepos.has(repo) || this.session?.did === repo
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

	/**
	 * Read a record from the PDS its repo lives on, not from HappyView.
	 * HappyView does not serve com.atproto.repo.getRecord: on this instance it
	 * forwards the call to the lexicon authority, which answers 400
	 * RecordNotFound for everything. Returns null only when the PDS says the
	 * record does not exist; anything else throws.
	 */
	private async fetchRecord(repo: string, collection: string, rkey: string): Promise<unknown | null> {
		const uri = `at://${repo}/${collection}/${rkey}`
		if (!this.canReadRepo(repo)) {
			throw new RecordReadNotAllowedError(repo)
		}
		const pds = await this.pdsResolver.resolve(repo)
		const url = `${pds}/xrpc/com.atproto.repo.getRecord?repo=${encodeURIComponent(repo)}&collection=${encodeURIComponent(collection)}&rkey=${encodeURIComponent(rkey)}`
		const response = await fetchWithRetry('getRecord', () => this.fetchImpl(url, {
			headers: { Accept: 'application/json' },
			redirect: 'error',
			signal: AbortSignal.timeout(SAFE_READ_TIMEOUT_MS),
		}), this.retry)

		let body: string
		try {
			body = await readCapped(response, MAX_RECORD_BYTES)
		} catch (err) {
			throw new RecordReadError(uri, 0, (err as Error).message)
		}

		if (response.ok) {
			try {
				return JSON.parse(body)
			} catch {
				throw new RecordReadError(uri, 0, 'response is not JSON')
			}
		}

		if (response.status === 404 || (response.status === 400 && /RecordNotFound/.test(body))) {
			return null
		}
		throw new RecordReadError(uri, response.status, body)
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
