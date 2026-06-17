import { AtpAgent } from '@atproto/api'

export interface GameLookupResult {
	uri: string
	record: Record<string, unknown>
	redirectedFrom?: string
}

export interface ContributionResult {
	uri: string
}

export class ContributionClient {
	private agent: AtpAgent
	private happyviewUrl: string
	private happyviewApiKey: string
	private did: string = ''

	constructor(happyviewUrl: string, happyviewApiKey: string) {
		this.happyviewUrl = happyviewUrl
		this.happyviewApiKey = happyviewApiKey
		this.agent = new AtpAgent({ service: happyviewUrl })
	}

	async login(identifier: string, password: string): Promise<void> {
		const result = await this.agent.login({ identifier, password })
		this.did = result.data.did
		console.log(`[contributions] Logged in as ${this.did} (${identifier})`)
	}

	/**
	 * Look up a game by IGDB ID via HappyView's getGame XRPC.
	 * Returns the current record (with redirect resolution) or null if not found.
	 */
	async getGameByIgdbId(igdbId: string): Promise<GameLookupResult | null> {
		const url = `${this.happyviewUrl}/xrpc/games.gamesgamesgamesgames.getGame?igdbId=${encodeURIComponent(igdbId)}`
		const response = await fetch(url, {
			headers: {
				'Authorization': `Bearer ${this.happyviewApiKey}`,
			},
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

	/**
	 * Create a contribution via HappyView's createContribution XRPC.
	 * Authenticated as the IGDB scraper identity.
	 */
	async createContribution(params: {
		contributionType: 'correction' | 'addition' | 'newGame'
		changes: Record<string, unknown>
		subject?: string
	}): Promise<ContributionResult> {
		const accessJwt = this.agent.session?.accessJwt
		if (!accessJwt) {
			throw new Error('Not authenticated — call login() first')
		}

		const body: Record<string, unknown> = {
			contributionType: params.contributionType,
			changes: params.changes,
		}
		if (params.subject) {
			body.subject = params.subject
		}

		const response = await fetch(
			`${this.happyviewUrl}/xrpc/games.gamesgamesgamesgames.createContribution`,
			{
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'Authorization': `Bearer ${accessJwt}`,
				},
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
