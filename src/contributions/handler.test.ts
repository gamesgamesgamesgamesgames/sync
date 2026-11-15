import { describe, test, expect, vi } from 'vitest'
import { syncGameViaContribution } from './handler.js'
import { ContributionRequestError, type ContributionClient } from './client.js'

const GAME_URI = 'at://did:web:gamesgamesgamesgames.games/games.gamesgamesgamesgames.game/g1'

function fakeClient(overrides: Partial<Record<keyof ContributionClient, unknown>> = {}) {
	return {
		getGameByIgdbId: vi.fn(async () => null),
		getGameRecord: vi.fn(async () => null),
		getRecord: vi.fn(async () => null),
		createContribution: vi.fn(async () => ({ uri: 'at://did:plc:scraper/games.gamesgamesgamesgames.contribution/x' })),
		canReadRepo: vi.fn((did: string) => did === 'did:web:gamesgamesgamesgames.games'),
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

describe('syncGameViaContribution unreadable records and owners', () => {
	test('a game whose record the PDS cannot find throws a RecordUnreadableError', async () => {
		const { RecordUnreadableError } = await import('./handler.js')
		const client = fakeClient({ getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: { name: 'Portal' } })) })
		await expect(syncGameViaContribution('602', { name: 'Portal' }, client)).rejects.toBeInstanceOf(RecordUnreadableError)
	})

	test('a game in a repo we may not read is skipped without any read', async () => {
		const owned = 'at://did:plc:studio/games.gamesgamesgamesgames.game/s1'
		const client = fakeClient({ getGameByIgdbId: vi.fn(async () => ({ uri: owned, record: { name: 'Owned' } })) })
		const result = await syncGameViaContribution('9', { name: 'Owned', summary: 'x' }, client)
		expect(result.action).toBe('skipped')
		expect(client.getGameRecord).not.toHaveBeenCalled()
		expect(client.createContribution).not.toHaveBeenCalled()
	})
})

describe('correction rkeys', () => {
	const changesFor = (client: { createContribution: ReturnType<typeof vi.fn> }) => client.createContribution.mock.calls[0]![0] as { rkey: string; changes: unknown }

	test('the same diff against a different base gets a different rkey', async () => {
		const a = fakeClient({
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: {} })),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, cid: 'bafyBaseOne', value: { name: 'Portal', summary: 'old' } })),
		})
		const b = fakeClient({
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: {} })),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, cid: 'bafyBaseTwo', value: { name: 'Portal', summary: 'old' } })),
		})
		await syncGameViaContribution('1', { name: 'Portal', summary: 'new' }, a)
		await syncGameViaContribution('1', { name: 'Portal', summary: 'new' }, b)
		expect(changesFor(a).changes).toEqual(changesFor(b).changes)
		expect(changesFor(a).rkey).not.toBe(changesFor(b).rkey)
	})

	test('the same diff against the same base keeps its rkey, so a rerun dedupes', async () => {
		const make = () => fakeClient({
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: {} })),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, cid: 'bafyBase', value: { summary: 'old' } })),
		})
		const a = make(); const b = make()
		await syncGameViaContribution('1', { summary: 'new' }, a)
		await syncGameViaContribution('1', { summary: 'new' }, b)
		expect(changesFor(a).rkey).toBe(changesFor(b).rkey)
	})

	test('newGame rkeys are the plain content hash', async () => {
		const { contentHashRkey } = await import('./hash.js')
		const client = fakeClient()
		await syncGameViaContribution('9', { name: 'Brand New', applicationType: 'game' }, client)
		expect(changesFor(client).rkey).toBe(contentHashRkey({ name: 'Brand New', applicationType: 'game' }))
	})
})

describe('BlobNotFound recovery', () => {
	const media = [{ blob: { $type: 'blob', ref: { $link: 'bafyM' }, mimeType: 'image/jpeg', size: 1 }, mediaType: 'cover', igdbImageId: 'coM' }]
	const blobNotFound = () => new ContributionRequestError('createContribution', 500,
		'{"error":"script_error","message":"runtime error: PDS createRecord returned 400 Bad Request: {\\"error\\":\\"BlobNotFound\\",\\"message\\":\\"Could not find blob: bafyM\\"}"}')
	const existing = () => ({
		getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: {} })),
		getGameRecord: vi.fn(async () => ({ uri: GAME_URI, cid: 'bafyBase', value: { summary: 'old' } })),
	})

	test('forgets the media blobs and retries once without media; the other fields land', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const onMissing = vi.fn()
		let n = 0
		const client = fakeClient({ ...existing(), createContribution: vi.fn(async () => { if (++n === 1) throw blobNotFound(); return { uri: 'at://x/c/2' } }) })
		const result = await syncGameViaContribution('1', { summary: 'new', media }, client, { onMissingContributorBlobs: onMissing })
		expect(result.action).toBe('created')
		expect(onMissing).toHaveBeenCalledWith(['coM'])
		expect(client.createContribution).toHaveBeenCalledTimes(2)
		expect((client.createContribution.mock.calls[1]![0] as { changes: unknown }).changes).toEqual({ summary: 'new' })
	})

	test('a media-only change is skipped after forgetting its blobs', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const onMissing = vi.fn()
		const client = fakeClient({ ...existing(), createContribution: vi.fn(async () => { throw blobNotFound() }) })
		const result = await syncGameViaContribution('1', { summary: 'old', media }, client, { onMissingContributorBlobs: onMissing })
		expect(result.action).toBe('skipped')
		expect(onMissing).toHaveBeenCalledWith(['coM'])
		expect(client.createContribution).toHaveBeenCalledTimes(1)
	})

	test('retries only once', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const client = fakeClient({ ...existing(), createContribution: vi.fn(async () => { throw blobNotFound() }) })
		await expect(syncGameViaContribution('1', { summary: 'new', media }, client)).rejects.toBeInstanceOf(ContributionRequestError)
		expect(client.createContribution).toHaveBeenCalledTimes(2)
	})

	test('other errors are not retried', async () => {
		const client = fakeClient({ ...existing(), createContribution: vi.fn(async () => { throw new ContributionRequestError('createContribution', 500, 'script_error: something else') }) })
		await expect(syncGameViaContribution('1', { summary: 'new', media }, client)).rejects.toBeInstanceOf(ContributionRequestError)
		expect(client.createContribution).toHaveBeenCalledTimes(1)
	})

	test('a newGame retries without media too', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		let n = 0
		const client = fakeClient({ createContribution: vi.fn(async () => { if (++n === 1) throw blobNotFound(); return { uri: 'at://x/c/3' } }) })
		const result = await syncGameViaContribution('9', { name: 'New', applicationType: 'game', media }, client)
		expect(result).toMatchObject({ action: 'created', contributionType: 'newGame' })
		expect((client.createContribution.mock.calls[1]![0] as { changes: unknown }).changes).toEqual({ name: 'New', applicationType: 'game' })
	})
})
