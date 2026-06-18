import { HappyViewNodeClient } from '@happyview/oauth-client-node'
import { FileStorage } from './storage.js'

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
}

export class ContributionClient {
	private hvClient: HappyViewNodeClient
	private session: Awaited<ReturnType<HappyViewNodeClient['restore']>> | null = null
	private instanceUrl: string
	private clientKey: string

	constructor(options: ContributionClientOptions) {
		this.instanceUrl = options.instanceUrl
		this.clientKey = options.clientKey
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

		const response = await this.session.fetchHandler(
			`${this.instanceUrl}/xrpc/com.atproto.repo.uploadBlob`,
			{
				method: 'POST',
				headers: { 'Content-Type': mimeType },
				body: Buffer.from(data),
			},
		)

		if (!response.ok) {
			const errorBody = await response.text()
			throw new Error(`uploadBlob returned ${response.status}: ${errorBody}`)
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
		const response = await fetch(url, {
			headers: { 'X-Client-Key': this.clientKey },
		})

		if (response.status === 404 || response.status === 400) {
			return null
		}

		if (!response.ok) {
			const body = await response.text()
			throw new Error(`getGame returned ${response.status}: ${body}`)
		}

		const data = await response.json() as { game: Record<string, unknown> & { uri: string; redirectedFrom?: string } }
		return {
			uri: data.game.uri,
			record: data.game,
			redirectedFrom: data.game.redirectedFrom,
		}
	}

	async getRecord(collection: string, rkey: string): Promise<Record<string, unknown> | null> {
		if (!this.session) {
			throw new Error('Not authenticated — call restore() first')
		}

		const response = await fetch(
			`${this.instanceUrl}/xrpc/com.atproto.repo.getRecord?repo=${encodeURIComponent(this.session.did)}&collection=${encodeURIComponent(collection)}&rkey=${encodeURIComponent(rkey)}`,
			{ headers: { 'X-Client-Key': this.clientKey } },
		)

		if (response.status === 404 || response.status === 400) {
			return null
		}

		if (!response.ok) {
			const body = await response.text()
			throw new Error(`getRecord returned ${response.status}: ${body}`)
		}

		return await response.json() as Record<string, unknown>
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

		const response = await this.session.fetchHandler(
			`${this.instanceUrl}/xrpc/games.gamesgamesgamesgames.createContribution`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(body),
			},
		)

		if (!response.ok) {
			const errorBody = await response.text()
			throw new Error(`createContribution returned ${response.status}: ${errorBody}`)
		}

		return await response.json() as ContributionResult
	}
}
