import { describe, test, expect, vi } from 'vitest'
import { syncGameViaContribution } from './handler.js'
import type { ContributionClient } from './client.js'

const GAME_URI = 'at://did:web:gamesgamesgamesgames.games/games.gamesgamesgamesgames.game/g1'

function fakeClient(overrides: Partial<Record<keyof ContributionClient, unknown>> = {}) {
	return {
		getGameByIgdbId: vi.fn(async () => null),
		getGameRecord: vi.fn(async () => null),
		getRecord: vi.fn(async () => null),
		createContribution: vi.fn(async () => ({ uri: 'at://did:plc:scraper/games.gamesgamesgamesgames.contribution/x' })),
		...overrides,
	} as unknown as ContributionClient & {
		createContribution: ReturnType<typeof vi.fn>
		getGameRecord: ReturnType<typeof vi.fn>
	}
}

describe('syncGameViaContribution', () => {
	test('diffs against the stored record, not the hydrated getGame view', async () => {
		const releases = [{ platformURI: 'at://p', releaseDates: [{ region: 'worldwide', releasedAt: 'TBD' }] }]
		const client = fakeClient({
			// getGame hydrates each release with its platform name.
			getGameByIgdbId: vi.fn(async () => ({
				uri: GAME_URI,
				record: { name: 'Portal', releases: [{ ...releases[0], platform: 'PC' }] },
			})),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, value: { name: 'Portal', releases } })),
		})

		const result = await syncGameViaContribution('1', { name: 'Portal', releases }, client)

		expect(result.action).toBe('skipped')
		expect(client.getGameRecord).toHaveBeenCalledWith(GAME_URI)
		expect(client.createContribution).not.toHaveBeenCalled()
	})

	test('creates a correction with only the fields that differ from the stored record', async () => {
		const client = fakeClient({
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: { name: 'Portal', platformNames: ['PC'] } })),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, value: { name: 'Portal', summary: 'old' } })),
		})

		const result = await syncGameViaContribution('1', { name: 'Portal', summary: 'new' }, client)

		expect(result).toMatchObject({ action: 'created', contributionType: 'correction' })
		expect(client.createContribution).toHaveBeenCalledWith(expect.objectContaining({
			contributionType: 'correction',
			subject: GAME_URI,
			changes: { summary: 'new' },
		}))
	})

	test('throws when getGame finds a game whose record cannot be read', async () => {
		const client = fakeClient({
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: { name: 'Portal' } })),
		})

		await expect(syncGameViaContribution('1', { name: 'Portal' }, client)).rejects.toThrow(/could not be read/)
	})

	test('a newGame carries only contributable fields', async () => {
		const client = fakeClient()

		await syncGameViaContribution('9', {
			$type: 'games.gamesgamesgamesgames.game',
			name: 'Brand New',
			applicationType: 'game',
			createdAt: '2026-11-13T00:00:00Z',
			publishedAt: '2026-11-01T00:00:00Z',
			externalIds: { igdb: '9' },
		}, client)

		expect(client.createContribution).toHaveBeenCalledWith(expect.objectContaining({
			contributionType: 'newGame',
			changes: { name: 'Brand New', applicationType: 'game', externalIds: { igdb: '9' } },
		}))
	})
})
