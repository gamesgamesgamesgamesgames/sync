/**
 * Media download and upload pipeline.
 *
 * Downloads images from IGDB's CDN and uploads them as blobs to atproto,
 * with caching to avoid re-uploads on resume.
 */

import type { IGDBClient } from './igdb/client.js'
import type { StateManager } from './state.js'
import { ConcurrencyPool } from './concurrency.js'

export interface UploadedBlob { ref: { $link: string }; mimeType: string; size: number }

export interface UploadOptions {
	/** The IGDB image the bytes came from, when there is one. */
	imageId?: string
}

/** Any object that can upload blobs — satisfied by both AtprotoClient and ContributionClient. */
export interface BlobUploader {
	uploadBlob(data: Uint8Array, mimeType: string, options?: UploadOptions): Promise<UploadedBlob>
	/** Optional: whether a blob already in the cache still has to reach another repo. */
	needsMirror?(imageId: string, cid: string): boolean
	/**
	 * Optional: send a cached blob's bytes to the repo that lacks them.
	 * `expectedCid` is the cached CID. Returns the blob to use from now on
	 * when it differs from the cached one (the bytes changed), else nothing.
	 */
	mirrorCached?(imageId: string, data: Uint8Array, mimeType: string, expectedCid: string): Promise<UploadedBlob | undefined>
	/** Optional: a cheap check that runs before any download; true means nothing to copy. */
	settleFromRemote?(imageId: string, cachedCid: string): Promise<boolean>
	/** Optional: whether every listed image's blob is confirmed in the contributor's repo. */
	allInContributor?(refs: Array<{ imageId: string; cid: string }>): boolean
}

/** A repo the mirror copies blobs into. One attempt per blob, no waiting. */
export interface MirrorTarget {
	uploadBlobOnce(data: Uint8Array, mimeType: string): Promise<UploadedBlob>
}

/** Where the mirror records which cached blobs are in which repo. */
export interface MirrorLedger {
	/** The catalogue repo holds this image's blob at this CID. */
	isBlobMirrored(imageId: string, cid: string): boolean
	setBlobMirrored(imageId: string, cid: string): void
	/** The contributor's repo holds it at this CID. Optional so older ledgers still work. */
	isBlobInContributor?(imageId: string, cid: string): boolean
	setBlobInContributor?(imageId: string, cid: string): void
	clearBlobInContributor?(imageId: string): void
	clearBlobMirrored?(imageId: string): void
	/** Replace a cached blob (its bytes changed, so its CID did). */
	setBlob?(imageId: string, entry: UploadedBlob): void
}

/** Asks the contributor's PDS whether it already holds a blob. */
export interface ContributorBlobCheck {
	hasContributorBlob(cid: string): Promise<boolean>
}

/**
 * Fails loudly when the account the sync logged in with is not the catalogue
 * account. Mirrored media would otherwise land in some other repo and every
 * auto-accepted media change would fall back.
 */
export function assertCatalogueAccount(actualDid: string, expectedDid: string | undefined): void {
	if (!expectedDid) {
		throw new Error('PENTARACT_DID is not set: it must name the catalogue account game media is mirrored to')
	}
	if (actualDid !== expectedDid) {
		throw new Error(`ATPROTO_IDENTIFIER logs in as ${actualDid}, but PENTARACT_DID is ${expectedDid}: refusing to mirror media into the wrong repo`)
	}
}

/**
 * Uploads each blob to the contributor's repo and also to the catalogue repo.
 *
 * A contribution's media references blobs in the contributor's repo, but once
 * it is accepted the same media lands on a game in the catalogue repo, and a
 * PDS refuses a record that references a blob it does not hold. Uploading the
 * same bytes to both gives the same CID in both places. A failed mirror upload
 * only logs: auto-accept then applies the rest of the contribution and leaves
 * the game's media as it was.
 */
export class MirroringBlobUploader implements BlobUploader {
	constructor(
		private readonly primary: BlobUploader,
		private readonly mirror: MirrorTarget,
		private readonly ledger?: MirrorLedger,
		private readonly contributorCheck?: ContributorBlobCheck,
	) {}

	async uploadBlob(data: Uint8Array, mimeType: string, options?: UploadOptions): Promise<UploadedBlob> {
		const result = await this.primary.uploadBlob(data, mimeType, options)
		if (options?.imageId) this.ledger?.setBlobInContributor?.(options.imageId, result.ref.$link)
		await this.copy(data, mimeType, result.ref.$link, options?.imageId)
		return result
	}

	private inContributor(imageId: string, cid: string): boolean {
		// Without a contributor ledger, assume it is there (the old behaviour).
		return !this.ledger?.isBlobInContributor || this.ledger.isBlobInContributor(imageId, cid)
	}

	needsMirror(imageId: string, cid: string): boolean {
		if (!this.ledger) return false
		return !this.ledger.isBlobMirrored(imageId, cid) || !this.inContributor(imageId, cid)
	}

	/**
	 * A cached blob may be missing from either repo: before 2026-06-18 the
	 * sync uploaded to the catalogue repo only, and from then until mirroring
	 * existed, to the contributor's only. A contribution referencing a blob its
	 * own repo doesn't hold is refused (BlobNotFound), so the contributor copy
	 * is made with the normal retrying upload; the catalogue copy stays a
	 * one-shot best effort.
	 *
	 * When the re-upload gives a different CID (IGDB now serves different
	 * bytes, for instance images cached at older IMAGE_SIZES), the new blob is
	 * adopted: both ledger entries for the image are dropped, the cache takes
	 * the new blob, and each repo's entry is re-recorded only once that repo
	 * really holds the new CID.
	 */
	async mirrorCached(imageId: string, data: Uint8Array, mimeType: string, expectedCid: string): Promise<UploadedBlob | undefined> {
		let cid = expectedCid
		let adopted: UploadedBlob | undefined
		if (!this.inContributor(imageId, expectedCid)) {
			try {
				const uploaded = await this.primary.uploadBlob(data, mimeType, { imageId })
				if (uploaded.ref.$link !== expectedCid) {
					console.warn(`[media] Re-uploaded blob for ${imageId}: CID ${uploaded.ref.$link} replaces cached ${expectedCid}`)
					this.ledger?.clearBlobMirrored?.(imageId)
					this.ledger?.clearBlobInContributor?.(imageId)
					this.ledger?.setBlob?.(imageId, uploaded)
					adopted = uploaded
					cid = uploaded.ref.$link
				}
				// The upload returned this CID, so the contributor holds it.
				this.ledger?.setBlobInContributor?.(imageId, cid)
			} catch (err) {
				console.warn(`[media] Failed to upload cached blob ${expectedCid} to the contributor repo:`, (err as Error).message)
			}
		}
		if (this.ledger && !this.ledger.isBlobMirrored(imageId, cid)) {
			await this.copy(data, mimeType, cid, imageId)
		}
		return adopted
	}

	/**
	 * Before downloading anything: if the contributor's PDS already holds the
	 * cached blob (true for everything cached from 2026-06-18 on), record that
	 * and report whether anything is left to copy.
	 */
	async settleFromRemote(imageId: string, cachedCid: string): Promise<boolean> {
		if (!this.ledger || this.inContributor(imageId, cachedCid) || !this.contributorCheck) {
			return !this.needsMirror(imageId, cachedCid)
		}
		try {
			if (await this.contributorCheck.hasContributorBlob(cachedCid)) {
				this.ledger.setBlobInContributor?.(imageId, cachedCid)
			}
		} catch (err) {
			console.warn(`[media] Could not check blob ${cachedCid} on the contributor repo:`, (err as Error).message)
		}
		return !this.needsMirror(imageId, cachedCid)
	}

	/** Whether every image's blob, at that CID, is confirmed in the contributor's repo. */
	allInContributor(refs: Array<{ imageId: string; cid: string }>): boolean {
		return refs.every((r) => this.inContributor(r.imageId, r.cid))
	}

	/** One attempt, so a rate-limited catalogue PDS never stalls the sync. */
	private async copy(data: Uint8Array, mimeType: string, expectedCid: string | undefined, imageId: string | undefined): Promise<void> {
		try {
			const mirrored = await this.mirror.uploadBlobOnce(data, mimeType)
			if (expectedCid && mirrored.ref.$link !== expectedCid) {
				console.warn(`[media] Mirrored blob CID ${mirrored.ref.$link} differs from ${expectedCid}`)
				return
			}
			if (imageId) this.ledger?.setBlobMirrored(imageId, mirrored.ref.$link)
		} catch (err) {
			console.warn(`[media] Failed to mirror blob ${expectedCid ?? imageId ?? ''} to the catalogue repo:`, (err as Error).message)
		}
	}
}

/**
 * For a cache hit: when the uploader still needs this blob in one of the
 * repos (the contributor's or the catalogue's), download it again and copy it
 * there. Never throws; the cached ref stands either way.
 */
async function mirrorCacheHit(
	uploader: BlobUploader,
	igdb: IGDBClient,
	imageId: string,
	size: string,
	cached: { ref: { $link: string }; mimeType: string },
	runDownload: <R>(fn: () => Promise<R>) => Promise<R> = (fn) => fn(),
	runUpload: <R>(fn: () => Promise<R>) => Promise<R> = (fn) => fn(),
): Promise<UploadedBlob | undefined> {
	if (!uploader.needsMirror?.(imageId, cached.ref.$link) || !uploader.mirrorCached) return undefined
	if (uploader.settleFromRemote && await uploader.settleFromRemote(imageId, cached.ref.$link)) return undefined
	let bytes: Buffer
	try {
		bytes = await runDownload(() => igdb.downloadImage(imageId, size))
	} catch (err) {
		console.warn(`[igdb] Failed to download image ${imageId} to copy its cached blob:`, (err as Error).message)
		return undefined
	}
	return runUpload(() => uploader.mirrorCached!(imageId, bytes, cached.mimeType, cached.ref.$link))
}

/** A media blob a contribution referenced, with what is needed to fetch it again. */
export interface RestoreRef { imageId: string; cid: string; mimeType: string; mediaType: string }

/**
 * After the contributor's PDS refused a contribution for a missing blob (and
 * the ledger has forgotten those images), copy each blob to the contributor's
 * repo again so the same media can be retried at once. True only when every
 * blob is now confirmed there at the CID the media already names. A changed
 * CID is adopted for the next run but means this media can't be reused, so
 * that is false too.
 */
export async function restoreContributorBlobs(uploader: BlobUploader, igdb: IGDBClient, refs: RestoreRef[]): Promise<boolean> {
	if (refs.length === 0 || !uploader.mirrorCached || !uploader.allInContributor) return false
	for (const r of refs) {
		const sizeKey = r.mediaType === 'cover' ? 'cover'
			: r.mediaType === 'screenshot' ? 'screenshot'
			: r.mediaType === 'icon' ? 'logo'
			: 'artwork'
		let bytes: Buffer
		try {
			bytes = await igdb.downloadImage(r.imageId, IMAGE_SIZES[sizeKey])
		} catch (err) {
			console.warn(`[igdb] Failed to download image ${r.imageId} to restore its blob:`, (err as Error).message)
			return false
		}
		if (await uploader.mirrorCached(r.imageId, bytes, r.mimeType, r.cid)) return false
	}
	return uploader.allInContributor(refs)
}

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
	atproto: BlobUploader,
	state: StateManager,
): Promise<BlobRef | null> {
	// Check cache first
	if (state.hasBlob(imageId)) {
		const cachedEntry = state.getBlob(imageId)!
		const cached = (await mirrorCacheHit(atproto, igdb, imageId, size, cachedEntry)) ?? cachedEntry
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
		const blobResult = await atproto.uploadBlob(imageBuffer, 'image/jpeg', { imageId })

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
	atproto: BlobUploader,
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
	atproto: BlobUploader,
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
				const cachedEntry = state.getBlob(task.imageId)!
				const adopted = await mirrorCacheHit(atproto, igdb, task.imageId, size, cachedEntry, (fn) => dlPool.run(fn), (fn) => ulPool.run(fn))
				const cached = adopted ?? cachedEntry
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
				const blobResult = await ulPool.run(() => atproto.uploadBlob(imageBuffer, 'image/jpeg', { imageId: task.imageId }))
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
