/**
 * Media download and upload pipeline.
 *
 * Downloads images from IGDB's CDN and uploads them as blobs to atproto,
 * with caching to avoid re-uploads on resume.
 */

import type { IGDBClient } from './igdb/client.js'
import type { AtprotoClient } from './atproto/client.js'
import type { StateManager } from './state.js'
import { ConcurrencyPool } from './concurrency.js'

/** The blob reference shape used in atproto records. */
export interface BlobRef {
	$type: 'blob'
	ref: { $link: string }
	mimeType: string
	size: number
}

/** A fully assembled media item ready for a lexicon record. */
export interface MediaItem {
	blob: BlobRef
	mediaType: string
	igdbImageId?: string
	width?: number
	height?: number
	title?: string
	description?: string
}

/** IGDB image size presets */
export const IMAGE_SIZES = {
	cover: 'cover_big',
	screenshot: '1080p',
	artwork: '1080p',
	logo: 'logo_med',
} as const

/**
 * Download an IGDB image and upload it as an atproto blob.
 * Returns the blob ref, or null if the upload fails.
 * Uses the state blob cache to avoid duplicate uploads.
 */
export async function uploadIGDBImage(
	imageId: string,
	size: string,
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<BlobRef | null> {
	// Check cache first
	if (state.hasBlob(imageId)) {
		const cached = state.getBlob(imageId)!
		return {
			$type: 'blob',
			ref: cached.ref,
			mimeType: cached.mimeType,
			size: cached.size,
		}
	}

	let imageBuffer: Buffer
	try {
		imageBuffer = await igdb.downloadImage(imageId, size)
	} catch (err) {
		console.warn(`[igdb] Failed to download image ${imageId}:`, (err as Error).message)
		return null
	}

	try {
		const blobResult = await atproto.uploadBlob(imageBuffer, 'image/jpeg')

		// Cache the result
		state.setBlob(imageId, blobResult)

		return {
			$type: 'blob',
			ref: blobResult.ref,
			mimeType: blobResult.mimeType,
			size: blobResult.size,
		}
	} catch (err) {
		console.warn(`[atproto] Failed to upload image ${imageId}:`, (err as Error).message)
		return null
	}
}

/**
 * Build a media item from an IGDB image entity (cover, screenshot, artwork, logo).
 */
export async function buildMediaItem(
	imageId: string,
	mediaType: string,
	width: number | undefined,
	height: number | undefined,
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
	title?: string,
): Promise<MediaItem | null> {
	const sizeKey = mediaType === 'cover' ? 'cover'
		: mediaType === 'screenshot' ? 'screenshot'
		: mediaType === 'icon' ? 'logo'
		: 'artwork'

	const size = IMAGE_SIZES[sizeKey as keyof typeof IMAGE_SIZES] ?? 'screenshot_huge'
	const blobRef = await uploadIGDBImage(imageId, size, igdb, atproto, state)

	if (!blobRef) return null

	const item: MediaItem = {
		blob: blobRef,
		mediaType,
		igdbImageId: imageId,
	}

	if (width) item.width = width
	if (height) item.height = height
	if (title) item.title = title

	return item
}

/** Describes a single image to process concurrently. */
export interface ImageTask {
	imageId: string
	mediaType: string
	width?: number
	height?: number
	title?: string
}

// Shared pools — created once, reused across calls
let downloadPool: ConcurrencyPool | null = null
let uploadPool: ConcurrencyPool | null = null

function getDownloadPool(): ConcurrencyPool {
	if (!downloadPool) downloadPool = new ConcurrencyPool(50)
	return downloadPool
}

function getUploadPool(): ConcurrencyPool {
	if (!uploadPool) uploadPool = new ConcurrencyPool(30)
	return uploadPool
}

/**
 * Process multiple image tasks concurrently using shared download/upload pools.
 * Each task: check blob cache -> download (via download pool) -> upload (via upload pool).
 * Returns MediaItem[] in the same order as tasks, with nulls filtered out.
 */
export async function buildMediaItemsConcurrent(
	tasks: ImageTask[],
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<MediaItem[]> {
	const dlPool = getDownloadPool()
	const ulPool = getUploadPool()

	const results = await Promise.all(
		tasks.map(async (task) => {
			const sizeKey = task.mediaType === 'cover' ? 'cover'
				: task.mediaType === 'screenshot' ? 'screenshot'
				: task.mediaType === 'icon' ? 'logo'
				: 'artwork'
			const size = IMAGE_SIZES[sizeKey as keyof typeof IMAGE_SIZES] ?? 'screenshot_huge'

			// Check cache first (no pool needed)
			if (state.hasBlob(task.imageId)) {
				const cached = state.getBlob(task.imageId)!
				const blobRef: BlobRef = {
					$type: 'blob',
					ref: cached.ref,
					mimeType: cached.mimeType,
					size: cached.size,
				}
				const item: MediaItem = { blob: blobRef, mediaType: task.mediaType, igdbImageId: task.imageId }
				if (task.width) item.width = task.width
				if (task.height) item.height = task.height
				if (task.title) item.title = task.title
				return item
			}

			// Download via download pool
			let imageBuffer: Buffer
			try {
				imageBuffer = await dlPool.run(() => igdb.downloadImage(task.imageId, size))
			} catch (err) {
				console.warn(`[igdb] Failed to download image ${task.imageId}:`, (err as Error).message)
				return null
			}

			// Upload via upload pool
			try {
				const blobResult = await ulPool.run(() => atproto.uploadBlob(imageBuffer, 'image/jpeg'))
				state.setBlob(task.imageId, blobResult)

				const blobRef: BlobRef = {
					$type: 'blob',
					ref: blobResult.ref,
					mimeType: blobResult.mimeType,
					size: blobResult.size,
				}
				const item: MediaItem = { blob: blobRef, mediaType: task.mediaType, igdbImageId: task.imageId }
				if (task.width) item.width = task.width
				if (task.height) item.height = task.height
				if (task.title) item.title = task.title
				return item
			} catch (err) {
				console.warn(`[atproto] Failed to upload image ${task.imageId}:`, (err as Error).message)
				return null
			}
		}),
	)

	return results.filter((item): item is MediaItem => item !== null)
}
