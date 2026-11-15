import { describe, test, expect, vi, afterEach } from 'vitest'
import { MirroringBlobUploader, assertCatalogueAccount, buildMediaItemsConcurrent, uploadIGDBImage, type BlobUploader, type MirrorTarget, type MirrorLedger } from './media.js'
import { mapGame } from './atproto/mapping.js'
import type { AtprotoClient } from './atproto/client.js'
import type { IGDBClient } from './igdb/client.js'
import type { StateManager } from './state.js'

const blob = (cid: string) => ({ ref: { $link: cid }, mimeType: 'image/jpeg', size: 3 })

function ledger(mirrored: string[] = []): MirrorLedger & { set: Set<string> } {
	const set = new Set(mirrored)
	return { set, isBlobMirrored: (id) => set.has(id), setBlobMirrored: (id) => { set.add(id) } }
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
	function fullLedger(opts: { mirrored?: string[]; contributor?: string[] } = {}) {
		const mirrored = new Set(opts.mirrored ?? [])
		const contributor = new Set(opts.contributor ?? [])
		return {
			mirrored, contributor,
			isBlobMirrored: (id: string) => mirrored.has(id),
			setBlobMirrored: (id: string) => { mirrored.add(id) },
			isBlobInContributor: (id: string) => contributor.has(id),
			setBlobInContributor: (id: string) => { contributor.add(id) },
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

	test('a contributor re-upload with a different CID is not recorded', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafy-other')) }
		const l = fullLedger({ mirrored: ['sc1'] })
		await buildMediaItemsConcurrent([{ imageId: 'sc1', mediaType: 'screenshot' }], igdb,
			new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn() }, l), cacheState(['sc1']))
		expect(l.contributor.has('sc1')).toBe(false)
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
		expect(s.isBlobInContributor('co1')).toBe(false)
		s.setBlobInContributor('co1')
		expect(s.isBlobInContributor('co1')).toBe(true)
		s.close()
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
