import { describe, test, expect, vi, afterEach } from 'vitest'
import { MirroringBlobUploader, assertCatalogueAccount, buildMediaItemsConcurrent, uploadIGDBImage, type BlobUploader, type MirrorTarget, type MirrorLedger } from './media.js'
import { mapGame } from './atproto/mapping.js'
import type { AtprotoClient } from './atproto/client.js'
import type { IGDBClient } from './igdb/client.js'
import type { StateManager } from './state.js'

const blob = (cid: string) => ({ ref: { $link: cid }, mimeType: 'image/jpeg', size: 3 })

function ledger(mirrored: string[] = []): MirrorLedger & { set: Set<string> } {
	const set = new Set(mirrored)
	return { set, isBlobMirrored: (id, _cid) => set.has(id), setBlobMirrored: (id, _cid) => { set.add(id) } }
}

afterEach(() => vi.restoreAllMocks())

describe('MirroringBlobUploader', () => {
	test('uploads to both repos, returns the contributor blob, records the mirror', async () => {
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafyA')) }
		const l = ledger()
		const data = new Uint8Array([1, 2, 3])

		const result = await new MirroringBlobUploader(primary, mirror, l).uploadBlob(data, 'image/jpeg', { imageId: 'co1' })

		expect(result.ref.$link).toBe('bafyA')
		expect(mirror.uploadBlobOnce).toHaveBeenCalledWith(data, 'image/jpeg')
		expect(l.set.has('co1')).toBe(true)
	})

	test('a failed mirror upload does not fail the upload and is not recorded', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => { throw new Error('RateLimitExceeded') }) }
		const l = ledger()

		const result = await new MirroringBlobUploader(primary, mirror, l).uploadBlob(new Uint8Array([1]), 'image/jpeg', { imageId: 'co1' })

		expect(result.ref.$link).toBe('bafyA')
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.set.has('co1')).toBe(false)
	})

	test('a failed contributor upload still throws', async () => {
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => { throw new Error('401') }) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafyA')) }

		await expect(new MirroringBlobUploader(primary, mirror).uploadBlob(new Uint8Array([1]), 'image/jpeg')).rejects.toThrow('401')
		expect(mirror.uploadBlobOnce).not.toHaveBeenCalled()
	})
})

describe('uploadIGDBImage with a mirroring uploader', () => {
	function fakeState(cached: boolean): StateManager {
		return {
			hasBlob: () => cached,
			getBlob: () => blob('bafyCached'),
			setBlob: vi.fn(),
		} as unknown as StateManager
	}

	test('a cache hit not yet mirrored downloads the image and mirrors it', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([9])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('x')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafyCached')) }
		const l = ledger()
		const uploader = new MirroringBlobUploader(primary, mirror, l)

		const ref = await uploadIGDBImage('co9', 'cover_big', igdb, uploader, fakeState(true))

		expect(ref?.ref.$link).toBe('bafyCached')
		expect(primary.uploadBlob).not.toHaveBeenCalled()
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.set.has('co9')).toBe(true)
	})

	test('a cache-hit mirror is checked against the cached CID', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([9])) } as unknown as IGDBClient
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafyDifferent')) }
		const l = ledger()
		await uploadIGDBImage('co9', 'cover_big', igdb, new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, mirror, l), fakeState(true))
		expect(l.set.has('co9')).toBe(false)
	})

	test('a cache hit already mirrored downloads nothing', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([9])) } as unknown as IGDBClient
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafyCached')) }
		const uploader = new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, mirror, ledger(['co9']))

		await uploadIGDBImage('co9', 'cover_big', igdb, uploader, fakeState(true))

		expect(igdb.downloadImage).not.toHaveBeenCalled()
		expect(mirror.uploadBlobOnce).not.toHaveBeenCalled()
	})

	test('a plain uploader on a cache hit does nothing extra', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const plain: BlobUploader = { uploadBlob: vi.fn() }
		const ref = await uploadIGDBImage('co9', 'cover_big', igdb, plain, fakeState(true))
		expect(ref?.ref.$link).toBe('bafyCached')
		expect(igdb.downloadImage).not.toHaveBeenCalled()
	})
})

describe('the game media path (buildMediaItemsConcurrent, which mapGame uses)', () => {
	function cacheState(cachedIds: string[]): StateManager {
		const cache = new Map(cachedIds.map((id) => [id, blob(`bafy-${id}`)]))
		return {
			hasBlob: (id: string) => cache.has(id),
			getBlob: (id: string) => cache.get(id),
			setBlob: vi.fn((id: string, b: ReturnType<typeof blob>) => { cache.set(id, b) }),
			getEntity: () => undefined,
		} as unknown as StateManager
	}

	test('a cache hit not yet mirrored is downloaded and mirrored with its cached CID', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('new')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-sc1')) }
		const l = ledger()

		const items = await buildMediaItemsConcurrent(
			[{ imageId: 'sc1', mediaType: 'screenshot' }],
			igdb, new MirroringBlobUploader(primary, mirror, l), cacheState(['sc1']),
		)

		expect(items[0]?.blob.ref.$link).toBe('bafy-sc1')
		expect(primary.uploadBlob).not.toHaveBeenCalled()
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.set.has('sc1')).toBe(true)
	})

	test('a cache hit whose mirrored CID differs is not recorded as mirrored', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-other')) }
		const l = ledger()

		await buildMediaItemsConcurrent(
			[{ imageId: 'sc1', mediaType: 'screenshot' }],
			igdb, new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, mirror, l), cacheState(['sc1']),
		)

		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.set.has('sc1')).toBe(false)
	})

	test('a fresh upload passes the image id so the ledger records the mirror', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-new')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-new')) }
		const l = ledger()

		await buildMediaItemsConcurrent(
			[{ imageId: 'co5', mediaType: 'cover' }],
			igdb, new MirroringBlobUploader(primary, mirror, l), cacheState([]),
		)

		expect(primary.uploadBlob).toHaveBeenCalledWith(expect.anything(), 'image/jpeg', { imageId: 'co5' })
		expect(l.set.has('co5')).toBe(true)
	})

	test('mapGame mirrors a cached cover through the real game path', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-co7')) }
		const l = ledger()
		const uploader = new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, mirror, l)

		const record = await mapGame(
			{ id: 7, name: 'Cover Only', cover: { id: 1, image_id: 'co7', width: 600, height: 800 } } as never,
			igdb, {} as AtprotoClient, cacheState(['co7']), { blobUploader: uploader },
		)

		expect((record.media as Array<{ blob: { ref: { $link: string } } }>)[0]?.blob.ref.$link).toBe('bafy-co7')
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.set.has('co7')).toBe(true)
	})
})

describe('cached blobs missing from the contributor repo', () => {
	/** Ledgers keyed by image id with the CID each copy was recorded at (cached CIDs are `bafy-<id>`). */
	function fullLedger(opts: { mirrored?: string[]; contributor?: string[] } = {}) {
		const mirrored = new Map((opts.mirrored ?? []).map((id) => [id, `bafy-${id}`]))
		const contributor = new Map((opts.contributor ?? []).map((id) => [id, `bafy-${id}`]))
		return {
			mirrored, contributor,
			isBlobMirrored: (id: string, cid: string) => mirrored.get(id) === cid,
			setBlobMirrored: (id: string, cid: string) => { mirrored.set(id, cid) },
			isBlobInContributor: (id: string, cid: string) => contributor.get(id) === cid,
			setBlobInContributor: (id: string, cid: string) => { contributor.set(id, cid) },
			clearBlobMirrored: (id: string) => { mirrored.delete(id) },
			clearBlobInContributor: (id: string) => { contributor.delete(id) },
		}
	}
	function cacheState(cachedIds: string[]): StateManager {
		const cache = new Map(cachedIds.map((id) => [id, blob(`bafy-${id}`)]))
		return {
			hasBlob: (id: string) => cache.has(id),
			getBlob: (id: string) => cache.get(id),
			setBlob: vi.fn(),
			getEntity: () => undefined,
		} as unknown as StateManager
	}

	test('mapGame re-uploads a cached cover the contributor repo lacks, with its cached CID, and records it', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-co7')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-co7')) }
		const l = fullLedger({ mirrored: ['co7'] })
		const record = await mapGame(
			{ id: 7, name: 'Old Cover', cover: { id: 1, image_id: 'co7', width: 600, height: 800 } } as never,
			igdb, {} as AtprotoClient, cacheState(['co7']), { blobUploader: new MirroringBlobUploader(primary, mirror, l) },
		)
		expect((record.media as Array<{ blob: { ref: { $link: string } } }>)[0]?.blob.ref.$link).toBe('bafy-co7')
		expect(primary.uploadBlob).toHaveBeenCalledTimes(1)
		expect(primary.uploadBlob).toHaveBeenCalledWith(expect.anything(), 'image/jpeg', { imageId: 'co7' })
		expect(mirror.uploadBlobOnce).not.toHaveBeenCalled()
		expect(l.contributor.has('co7')).toBe(true)
	})

	test('a cached blob in both repos is not downloaded or uploaded', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn() }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn() }
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, mirror, fullLedger({ mirrored: ['sc1'], contributor: ['sc1'] })), cacheState(['sc1']))
		expect(igdb.downloadImage).not.toHaveBeenCalled()
		expect(primary.uploadBlob).not.toHaveBeenCalled()
		expect(mirror.uploadBlobOnce).not.toHaveBeenCalled()
	})

	test('a cached blob missing from both repos goes to both', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-sc1')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-sc1')) }
		const l = fullLedger()
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb, new MirroringBlobUploader(primary, mirror, l), cacheState(['sc1']))
		expect(igdb.downloadImage).toHaveBeenCalledTimes(1)
		expect(l.contributor.has('sc1')).toBe(true)
		expect(l.mirrored.has('sc1')).toBe(true)
	})

	test('a contributor re-upload with a different CID is adopted everywhere', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-new')) }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-new')) }
		const setBlob = vi.fn()
		const l = { ...fullLedger({ mirrored: ['sc1'] }), setBlob, clearBlobMirrored: vi.fn((id: string) => { l.mirrored.delete(id) }) }
		const items = await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, mirror, l), cacheState(['sc1']))
		expect(items[0]?.blob.ref.$link).toBe('bafy-new')
		expect(setBlob).toHaveBeenCalledWith('sc1', blob('bafy-new'))
		expect(l.contributor.has('sc1')).toBe(true)
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.mirrored.has('sc1')).toBe(true)
	})

	test('after adoption, neither repo counts until it holds the new CID', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		// The catalogue copy fails, so only the contributor is re-recorded.
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => { throw new Error('RateLimitExceeded') }) }
		const l = fullLedger({ mirrored: ['sc1'] })
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader({ uploadBlob: vi.fn(async () => blob('bafy-new')) }, mirror, l), cacheState(['sc1']))
		expect(l.contributor.get('sc1')).toBe('bafy-new')
		expect(l.mirrored.has('sc1')).toBe(false)
		expect(l.isBlobMirrored('sc1', 'bafy-sc1')).toBe(false)
		expect(l.isBlobMirrored('sc1', 'bafy-new')).toBe(false)
	})

	test('an adopted CID is mirrored even when the ledger cannot forget the old mirror', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-new')) }
		const { clearBlobMirrored: _a, clearBlobInContributor: _b, ...noClear } = fullLedger({ mirrored: ['sc1'] })
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader({ uploadBlob: vi.fn(async () => blob('bafy-new')) }, mirror, noClear), cacheState(['sc1']))
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(noClear.mirrored.get('sc1')).toBe('bafy-new')
	})

	test('the pre-check skips download and upload when the contributor already has the blob', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn() }
		const check = { hasContributorBlob: vi.fn(async () => true) }
		const l = fullLedger({ mirrored: ['sc1'] })
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, l, check), cacheState(['sc1']))
		expect(check.hasContributorBlob).toHaveBeenCalledWith('bafy-sc1')
		expect(igdb.downloadImage).not.toHaveBeenCalled()
		expect(primary.uploadBlob).not.toHaveBeenCalled()
		expect(l.contributor.has('sc1')).toBe(true)
	})

	test('the pre-check still mirrors to the catalogue when only that copy is missing', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn() }
		const mirror: MirrorTarget = { uploadBlobOnce: vi.fn(async () => blob('bafy-sc1')) }
		const check = { hasContributorBlob: vi.fn(async () => true) }
		const l = fullLedger()
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, mirror, l, check), cacheState(['sc1']))
		expect(primary.uploadBlob).not.toHaveBeenCalled()
		expect(mirror.uploadBlobOnce).toHaveBeenCalledTimes(1)
		expect(l.contributor.has('sc1') && l.mirrored.has('sc1')).toBe(true)
	})

	test('a pre-check that says no, or fails, falls back to the upload', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		for (const check of [{ hasContributorBlob: vi.fn(async () => false) }, { hasContributorBlob: vi.fn(async () => { throw new Error('503') }) }]) {
			const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
			const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-sc1')) }
			const l = fullLedger({ mirrored: ['sc1'] })
			await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
				new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, l, check), cacheState(['sc1']))
			expect(primary.uploadBlob).toHaveBeenCalledTimes(1)
			expect(l.contributor.has('sc1')).toBe(true)
		}
	})

	test('mapGame leaves the whole media field out when a blob is not confirmed in the contributor repo', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => { throw new Error('uploadBlob returned 500') }) }
		const l = fullLedger({ mirrored: ['co7', 'sc8'], contributor: ['sc8'] })
		const record = await mapGame(
			{ id: 7, name: 'Half Confirmed', summary: 's', cover: { id: 1, image_id: 'co7' }, screenshots: [{ id: 2, image_id: 'sc8' }] } as never,
			igdb, {} as AtprotoClient, cacheState(['co7', 'sc8']), { blobUploader: new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, l) },
		)
		expect(record.media).toBeUndefined()
		expect(record.summary).toBe('s')
	})

	test('mapGame leaves media out when a fresh upload fails, rather than shortening the array', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		let n = 0
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => { if (++n === 2) throw new Error('500'); return blob(`bafy-${n}`) }) }
		const record = await mapGame(
			{ id: 8, name: 'Two Shots', screenshots: [{ id: 1, image_id: 'sa' }, { id: 2, image_id: 'sb' }] } as never,
			igdb, {} as AtprotoClient, cacheState([]), { blobUploader: new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn(async () => blob('x')) }, fullLedger()) },
		)
		expect(record.media).toBeUndefined()
	})

	test('control: the same two-screenshot game keeps both images when uploads succeed', async () => {
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		let n = 0
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob(`bafy-${++n}`)) }
		const record = await mapGame(
			{ id: 8, name: 'Two Shots', screenshots: [{ id: 1, image_id: 'sa' }, { id: 2, image_id: 'sb' }] } as never,
			igdb, {} as AtprotoClient, cacheState([]), { blobUploader: new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn(async () => blob('x')) }, fullLedger()) },
		)
		expect((record.media as unknown[]).length).toBe(2)
	})

	test('control: the half-confirmed game keeps both images once both are confirmed', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const record = await mapGame(
			{ id: 7, name: 'Half Confirmed', summary: 's', cover: { id: 1, image_id: 'co7' }, screenshots: [{ id: 2, image_id: 'sc8' }] } as never,
			igdb, {} as AtprotoClient, cacheState(['co7', 'sc8']),
			{ blobUploader: new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, { uploadBlobOnce: vi.fn() }, fullLedger({ mirrored: ['co7', 'sc8'], contributor: ['co7', 'sc8'] })) },
		)
		expect((record.media as unknown[]).length).toBe(2)
	})

	test('mapGame leaves media out when an image could not be built, even if the ledger knows it', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async (id: string) => { if (id === 'sb') throw new Error('IGDB 404'); return Buffer.from([1]) }) } as unknown as IGDBClient
		const record = await mapGame(
			{ id: 8, name: 'Lost Image', screenshots: [{ id: 1, image_id: 'sa' }, { id: 2, image_id: 'sb' }] } as never,
			igdb, {} as AtprotoClient, cacheState([]),
			{ blobUploader: new MirroringBlobUploader({ uploadBlob: vi.fn(async () => blob('bafy-a')) }, { uploadBlobOnce: vi.fn(async () => blob('bafy-a')) }, fullLedger({ contributor: ['sa', 'sb'] })) },
		)
		expect(record.media).toBeUndefined()
	})

	test('mapGame keeps media when every blob is confirmed', async () => {
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const record = await mapGame(
			{ id: 9, name: 'All Good', cover: { id: 1, image_id: 'co9' } } as never,
			igdb, {} as AtprotoClient, cacheState(['co9']),
			{ blobUploader: new MirroringBlobUploader({ uploadBlob: vi.fn() } as unknown as BlobUploader, { uploadBlobOnce: vi.fn() }, fullLedger({ mirrored: ['co9'], contributor: ['co9'] })) },
		)
		expect((record.media as unknown[]).length).toBe(1)
	})

	test('a failed contributor re-upload is logged, not thrown', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => { throw new Error('uploadBlob returned 500') }) }
		const items = await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, fullLedger({ mirrored: ['sc1'] })), cacheState(['sc1']))
		expect(items[0]?.blob.ref.$link).toBe('bafy-sc1')
	})

	test('a fresh upload records the contributor copy', async () => {
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-new')) }
		const l = fullLedger()
		await new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn(async () => blob('bafy-new')) }, l).uploadBlob(new Uint8Array([1]), 'image/jpeg', { imageId: 'co9' })
		expect(l.contributor.has('co9')).toBe(true)
	})
})

describe('StateManager blob_contributor', () => {
	test('round-trips', async () => {
		const { StateManager } = await import('./state.js')
		const { tmpdir } = await import('node:os')
		const { join } = await import('node:path')
		vi.spyOn(console, 'log').mockImplementation(() => {})
		const s = new StateManager(join(tmpdir(), `bc-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`))
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(false)
		s.setBlobInContributor('co1', 'bafyA')
		expect(s.isBlobInContributor('co1', 'bafyA')).toBe(true)
		expect(s.isBlobInContributor('co1', 'bafyB')).toBe(false)
		s.setBlobMirrored('co1', 'bafyA')
		expect(s.isBlobMirrored('co1', 'bafyA')).toBe(true)
		expect(s.isBlobMirrored('co1', 'bafyB')).toBe(false)
		s.clearBlobMirrored('co1'); s.clearBlobInContributor('co1')
		expect(s.isBlobMirrored('co1', 'bafyA') || s.isBlobInContributor('co1', 'bafyA')).toBe(false)
		s.close()
	})

	test('an existing ledger without a cid column is migrated, and its old rows count as unverified', async () => {
		const { StateManager } = await import('./state.js')
		const Database = (await import('better-sqlite3')).default
		const { tmpdir } = await import('node:os')
		const { join } = await import('node:path')
		vi.spyOn(console, 'log').mockImplementation(() => {})
		const path = join(tmpdir(), `bc-old-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`)
		const old = new Database(path)
		old.exec('CREATE TABLE blob_mirrors (image_id TEXT PRIMARY KEY); CREATE TABLE blob_contributor (image_id TEXT PRIMARY KEY);')
		old.prepare('INSERT INTO blob_mirrors (image_id) VALUES (?)').run('co1')
		old.prepare('INSERT INTO blob_contributor (image_id) VALUES (?)').run('co1')
		old.close()
		const s = new StateManager(path)
		expect(s.isBlobMirrored('co1', 'bafy-co1')).toBe(false)
		expect(s.isBlobInContributor('co1', 'bafy-co1')).toBe(false)
		s.setBlobMirrored('co1', 'bafy-co1')
		expect(s.isBlobMirrored('co1', 'bafy-co1')).toBe(true)
		s.close()
		// Reopening is idempotent.
		const again = new StateManager(path)
		expect(again.isBlobMirrored('co1', 'bafy-co1')).toBe(true)
		again.close()
	})
})

describe('assertCatalogueAccount', () => {
	test('passes when the DIDs match', () => {
		expect(() => assertCatalogueAccount('did:web:g', 'did:web:g')).not.toThrow()
	})

	test('throws when they differ', () => {
		expect(() => assertCatalogueAccount('did:plc:other', 'did:web:g')).toThrow(/wrong repo/)
	})

	test('throws when PENTARACT_DID is missing', () => {
		expect(() => assertCatalogueAccount('did:web:g', undefined)).toThrow(/PENTARACT_DID is not set/)
	})
})
