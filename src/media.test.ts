import { describe, test, expect, vi, afterEach } from 'vitest'
import { MirroringBlobUploader, assertCatalogueAccount, uploadIGDBImage, type BlobUploader, type MirrorTarget, type MirrorLedger } from './media.js'
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
