import { describe, test, expect, vi } from 'vitest'
import { MirroringBlobUploader, type BlobUploader } from './media.js'

const blob = (cid: string) => ({ ref: { $link: cid }, mimeType: 'image/jpeg', size: 3 })

describe('MirroringBlobUploader', () => {
	test('uploads to both repos and returns the contributor blob', async () => {
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		const mirror: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		const data = new Uint8Array([1, 2, 3])

		const result = await new MirroringBlobUploader(primary, mirror).uploadBlob(data, 'image/jpeg')

		expect(result.ref.$link).toBe('bafyA')
		expect(primary.uploadBlob).toHaveBeenCalledWith(data, 'image/jpeg')
		expect(mirror.uploadBlob).toHaveBeenCalledWith(data, 'image/jpeg')
	})

	test('a failed mirror upload does not fail the contribution upload', async () => {
		vi.spyOn(console, 'warn').mockImplementation(() => {})
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }
		const mirror: BlobUploader = { uploadBlob: vi.fn(async () => { throw new Error('RateLimitExceeded') }) }

		const result = await new MirroringBlobUploader(primary, mirror).uploadBlob(new Uint8Array([1]), 'image/jpeg')

		expect(result.ref.$link).toBe('bafyA')
		vi.restoreAllMocks()
	})

	test('a failed contributor upload still throws', async () => {
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => { throw new Error('401') }) }
		const mirror: BlobUploader = { uploadBlob: vi.fn(async () => blob('bafyA')) }

		await expect(new MirroringBlobUploader(primary, mirror).uploadBlob(new Uint8Array([1]), 'image/jpeg')).rejects.toThrow('401')
		expect(mirror.uploadBlob).not.toHaveBeenCalled()
	})
})
