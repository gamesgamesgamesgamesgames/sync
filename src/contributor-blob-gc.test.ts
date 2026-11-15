import { describe, test, expect, vi, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { StateManager, CONTRIBUTOR_BLOB_TRUST_SECONDS } from './state.js'
import { MirroringBlobUploader, restoreContributorBlobs, type BlobUploader, type MirrorLedger, type RestoreRef } from './media.js'
import { syncGameViaContribution } from './contributions/handler.js'
import { ContributionRequestError, type ContributionClient } from './contributions/client.js'
import type { IGDBClient } from './igdb/client.js'

const GAME_URI = 'at://did:web:gamesgamesgamesgames.games/games.gamesgamesgamesgames.game/g1'
const blob = (cid: string) => ({ ref: { $link: cid }, mimeType: 'image/jpeg', size: 3 })

afterEach(() => {
	vi.useRealTimers()
	vi.restoreAllMocks()
})

function freshState(): StateManager {
	vi.spyOn(console, 'log').mockImplementation(() => {})
	return new StateManager(join(tmpdir(), `gc-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`))
}

describe('contributor ledger trust window', () => {
	test('a confirmation is trusted for the window, then checked again', () => {
		vi.useFakeTimers({ toFake: ['Date'] })
		vi.setSystemTime(new Date('2026-11-15T00:00:00Z'))
		const s = freshState()
		s.setBlobInContributor('co1', 'bafyA')
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		vi.setSystemTime(new Date(Date.parse('2026-11-15T00:00:00Z') + CONTRIBUTOR_BLOB_TRUST_SECONDS * 1000))
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		vi.setSystemTime(new Date(Date.parse('2026-11-15T00:00:00Z') + (CONTRIBUTOR_BLOB_TRUST_SECONDS + 1) * 1000))
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(false)
		s.close()
	})

	test('the window stays under the PDS grace period of 6 hours', () => {
		expect(CONTRIBUTOR_BLOB_TRUST_SECONDS).toBeLessThan(6 * 3600)
	})

	test('a blob a contribution references is trusted for good, at that CID only', () => {
		vi.useFakeTimers({ toFake: ['Date'] })
		vi.setSystemTime(new Date('2026-11-15T00:00:00Z'))
		const s = freshState()
		s.setBlobInContributor('co1', 'bafyA')
		s.markContributorBlobsReferenced([{ imageId: 'co1', cid: 'bafyOther' }])
		s.markContributorBlobsReferenced([{ imageId: 'co1', cid: 'bafyA' }])
		vi.setSystemTime(new Date('2026-11-20T00:00:00Z'))
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		// Confirming the same CID again keeps it referenced; a new CID starts over.
		s.setBlobInContributor('co1', 'bafyA')
		vi.setSystemTime(new Date('2026-11-25T00:00:00Z'))
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		s.setBlobInContributor('co1', 'bafyB')
		vi.setSystemTime(new Date('2026-11-26T00:00:00Z'))
		expect(s.isBlobInContributor('co1', 'bafyB')).toBe(false)
		s.close()
	})

	test('rows from before the columns existed are checked again', () => {
		vi.spyOn(console, 'log').mockImplementation(() => {})
		const path = join(tmpdir(), `gc-old-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		const old = new Database(path)
		old.exec('CREATE TABLE blob_contributor (image_id TEXT PRIMARY KEY, cid TEXT);')
		old.prepare('INSERT INTO blob_contributor (image_id, cid) VALUES (?, ?)').run('co1', 'bafyA')
		old.close()
		const s = new StateManager(path)
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(false)
		s.setBlobInContributor('co1', 'bafyA')
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		s.close()
		const again = new StateManager(path)
		expect(again.isBlobInContributor('co1', 'bafyA')).toBe(true)
		again.close()
	})

	test('an expired row makes the uploader ask the contributor PDS again', async () => {
		vi.useFakeTimers({ toFake: ['Date'] })
		vi.setSystemTime(new Date('2026-11-15T00:00:00Z'))
		const s = freshState()
		s.setBlobInContributor('co1', 'bafyA')
		s.setBlobMirrored('co1', 'bafyA')
		const check = { hasContributorBlob: vi.fn(async () => false) }
		const uploader = new MirroringBlobUploader({ uploadBlob: vi.fn() }, { uploadBlobOnce: vi.fn() }, s, check)
		expect(await uploader.settleFromRemote('co1', 'bafyA')).toBe(true)
		expect(check.hasContributorBlob).not.toHaveBeenCalled()
		vi.setSystemTime(new Date('2026-11-16T00:00:00Z'))
		expect(await uploader.settleFromRemote('co1', 'bafyA')).toBe(false)
		expect(check.hasContributorBlob).toHaveBeenCalledWith('bafyA')
		s.close()
	})
})

describe('restoreContributorBlobs', () => {
	function ledger(): MirrorLedger & { contributor: Map<string, string> } {
		const contributor = new Map<string, string>()
		return {
			contributor,
			isBlobMirrored: () => true, setBlobMirrored: () => {},
			isBlobInContributor: (id, cid) => contributor.get(id) === cid,
			setBlobInContributor: (id, cid) => { contributor.set(id, cid) },
			clearBlobInContributor: (id) => { contributor.delete(id) },
			clearBlobMirrored: () => {},
			setBlob: () => {},
		} as MirrorLedger & { contributor: Map<string, string> }
	}
	const refs: RestoreRef[] = [
		{ imageId: 'co1', cid: 'bafyA', mimeType: 'image/jpeg', mediaType: 'cover' },
		{ imageId: 'sc1', cid: 'bafyB', mimeType: 'image/png', mediaType: 'screenshot' },
	]

	test('downloads each image at its size, uploads it to the contributor and confirms it', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const cids: Record<string, string> = { 'image/jpeg': 'bafyA', 'image/png': 'bafyB' }
		const primary: BlobUploader = { uploadBlob: vi.fn(async (_d: Uint8Array, mime: string) => blob(cids[mime]!)) }
		const l = ledger()
		const ok = await restoreContributorBlobs(new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, l), igdb, refs)
		expect(ok).toBe(true)
		expect(igdb.downloadImage).toHaveBeenCalledWith('co1', 'cover_big')
		expect(igdb.downloadImage).toHaveBeenCalledWith('sc1', '1080p')
		expect(primary.uploadBlob).toHaveBeenCalledTimes(2)
		expect(primary.uploadBlob).toHaveBeenCalledWith(expect.anything(), 'image/png', { imageId: 'sc1' })
		expect(l.contributor.get('sc1')).toBe('bafyB')
	})

	test('a changed CID means the media cannot be retried as it is', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyNew')) }
		expect(await restoreContributorBlobs(new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, ledger()), igdb, refs)).toBe(false)
		// It stops at the first changed CID instead of fetching the rest.
		expect(igdb.downloadImage).toHaveBeenCalledTimes(1)
	})

	test('a failed download or upload gives up', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const bad = { downloadImage: vi.fn(async () => { throw new Error('404') }) } as unknown as IGDBClient
		const good = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const ok = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		expect(await restoreContributorBlobs(new MirroringBlobUploader(ok, { uploadBlobOnce: vi.fn() }, ledger()), bad, refs)).toBe(false)
		const failing = { uploadBlob: vi.fn(async () => { throw new Error('uploadBlob returned 500') }) }
		expect(await restoreContributorBlobs(new MirroringBlobUploader(failing, { uploadBlobOnce: vi.fn() }, ledger()), good, refs)).toBe(false)
	})

	test('nothing to restore is not a success', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		expect(await restoreContributorBlobs(new MirroringBlobUploader({ uploadBlob: vi.fn() }, { uploadBlobOnce: vi.fn() }, ledger()), igdb, [])).toBe(false)
	})
})

describe('BlobNotFound: restore and retry with media', () => {
	const media = [
		{ blob: { $type: 'blob', ref: { $link: 'bafyM' }, mimeType: 'image/jpeg', size: 1 }, mediaType: 'cover', igdbImageId: 'coM' },
		{ blob: { $type: 'blob', ref: { $link: 'bafyS' }, mimeType: 'image/png', size: 1 }, mediaType: 'screenshot', igdbImageId: 'scS' },
	]
	const blobNotFound = () => new ContributionRequestError('createContribution', 500,
		'{"error":"script_error","message":"runtime error: PDS createRecord returned 400 Bad Request: {\\"error\\":\\"BlobNotFound\\",\\"message\\":\\"Could not find blob: bafyM\\"}"}')
	function client(failures: number, existingRecord: unknown = null) {
		let n = 0
		return {
			getGameByIgdbId: vi.fn(async () => ({ uri: GAME_URI, record: {} })),
			getGameRecord: vi.fn(async () => ({ uri: GAME_URI, cid: 'bafyBase', value: { summary: 'old' } })),
			getRecord: vi.fn(async () => existingRecord),
			canReadRepo: vi.fn(() => true),
			createContribution: vi.fn(async () => { if (++n <= failures) throw blobNotFound(); return { uri: 'at://x/c/' + n } }),
		} as unknown as ContributionClient & { createContribution: ReturnType<typeof vi.fn> }
	}
	const changesOf = (c: { createContribution: ReturnType<typeof vi.fn> }, i: number) => (c.createContribution.mock.calls[i]![0] as { changes: Record<string, unknown> }).changes

	test('restored blobs are retried with the same media, and marked as referenced', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const c = client(1)
		const onMissing = vi.fn()
		const restore = vi.fn(async () => true)
		const onReferenced = vi.fn()
		const result = await syncGameViaContribution('1', { summary: 'new', media }, c, {
			onMissingContributorBlobs: onMissing, restoreContributorBlobs: restore, onContributorBlobsReferenced: onReferenced,
		})
		expect(result.action).toBe('created')
		expect(onMissing).toHaveBeenCalledWith(['coM', 'scS'])
		expect(restore).toHaveBeenCalledWith([
			{ imageId: 'coM', cid: 'bafyM', mimeType: 'image/jpeg', mediaType: 'cover' },
			{ imageId: 'scS', cid: 'bafyS', mimeType: 'image/png', mediaType: 'screenshot' },
		])
		expect(c.createContribution).toHaveBeenCalledTimes(2)
		expect(changesOf(c, 1)).toEqual({ summary: 'new', media })
		expect(onReferenced).toHaveBeenCalledTimes(1)
		expect(onReferenced.mock.calls[0]![0].map((r: { imageId: string }) => r.imageId)).toEqual(['coM', 'scS'])
	})

	test('a second refusal falls back to without media: three attempts at most, one restore', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const c = client(2)
		const restore = vi.fn(async () => true)
		const result = await syncGameViaContribution('1', { summary: 'new', media }, c, { restoreContributorBlobs: restore })
		expect(result.action).toBe('created')
		expect(restore).toHaveBeenCalledTimes(1)
		expect(c.createContribution).toHaveBeenCalledTimes(3)
		expect(changesOf(c, 2)).toEqual({ summary: 'new' })
	})

	test('refused every time, it stops after three attempts', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const c = client(99)
		await expect(syncGameViaContribution('1', { summary: 'new', media }, c, { restoreContributorBlobs: vi.fn(async () => true) }))
			.rejects.toBeInstanceOf(ContributionRequestError)
		expect(c.createContribution).toHaveBeenCalledTimes(3)
	})

	test('blobs that could not be restored go without media, as before', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		for (const restore of [vi.fn(async () => false), vi.fn(async () => { throw new Error('boom') })]) {
			const c = client(1)
			const result = await syncGameViaContribution('1', { summary: 'new', media }, c, { restoreContributorBlobs: restore })
			expect(result.action).toBe('created')
			expect(c.createContribution).toHaveBeenCalledTimes(2)
			expect(changesOf(c, 1)).toEqual({ summary: 'new' })
		}
	})

	test('media with an item it cannot name is not restored', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const c = client(1)
		const restore = vi.fn(async () => true)
		await syncGameViaContribution('1', { summary: 'new', media: [...media, { blob: { ref: { $link: 'bafyX' } }, mediaType: 'artwork' }] }, c, { restoreContributorBlobs: restore })
		expect(restore).not.toHaveBeenCalled()
		expect(changesOf(c, 1)).toEqual({ summary: 'new' })
	})

	test('a contribution that already exists counts as referencing its blobs', async () => {
		const c = client(0, { value: {} })
		const onReferenced = vi.fn()
		const result = await syncGameViaContribution('1', { summary: 'new', media }, c, { onContributorBlobsReferenced: onReferenced })
		expect(result.action).toBe('skipped')
		expect(onReferenced).toHaveBeenCalledTimes(1)
	})

	test('a change without media marks nothing', async () => {
		const c = client(0)
		const onReferenced = vi.fn()
		await syncGameViaContribution('1', { summary: 'new' }, c, { onContributorBlobsReferenced: onReferenced })
		expect(onReferenced).not.toHaveBeenCalled()
	})
})
