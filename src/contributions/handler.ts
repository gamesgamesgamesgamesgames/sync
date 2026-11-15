import { diffGameRecord, pickContributableFields } from './diff.js'
import { contentHashRkey } from './hash.js'
import { ContributionRequestError, type ContributionClient } from './client.js'
import { blobCid } from '../atproto/lex-json.js'

/**
 * getGame found a game but its stored record could not be read: the PDS says
 * the record isn't there. That is the game's problem (index drift, a
 * deactivated repo), so it is charged to the game; a whole-path outage still
 * trips the breaker.
 */
export class RecordUnreadableError extends Error {
	constructor(readonly uri: string, readonly igdbId: string) {
		super(`getGame found ${uri} for IGDB ${igdbId}, but its record could not be read`)
		this.name = 'RecordUnreadableError'
	}
}

const CONTRIBUTION_COLLECTION = 'games.gamesgamesgamesgames.contribution'

export interface ContributionSyncResult {
	/** `held`: a new game not proposed yet, because it has no applicationType. */
	action: 'created' | 'skipped' | 'held'
	contributionType?: 'correction' | 'newGame'
	uri?: string
}

export interface SyncGameOptions {
	/**
	 * Called with the IGDB image ids of a contribution's media when the
	 * contributor's PDS refused it for a missing blob, so the ledger stops
	 * claiming those blobs are there and a later run copies them again.
	 */
	onMissingContributorBlobs?: (imageIds: string[]) => void
	/**
	 * Whether this image's blob, at this CID, is recorded as mirrored to the
	 * catalogue repo. The ledger is only written when the catalogue's upload
	 * returned that exact CID, which is trusted as is: the catalogue runs the
	 * reference PDS, whose sync.getBlob answers BlobNotFound for blobs no record
	 * references yet, so it can't confirm a fresh mirror.
	 */
	isImageMirrored?: (imageId: string, cid: string) => boolean
	/**
	 * Copy these blobs to the contributor's repo again after a missing-blob
	 * refusal; true when the same media can be retried. Without it, or when it
	 * returns false, the contribution goes ahead without media.
	 */
	restoreContributorBlobs?: (refs: MediaRef[]) => Promise<boolean>
	/** Told which blobs a created (or already existing) contribution references. */
	onContributorBlobsReferenced?: (refs: MediaRef[]) => void
}

export interface MediaRef { imageId: string; cid: string; mimeType: string; mediaType: string }

function mediaRefs(changes: Record<string, unknown>): MediaRef[] {
	if (!Array.isArray(changes.media)) return []
	const refs: MediaRef[] = []
	for (const item of changes.media as Array<Record<string, unknown>>) {
		const imageId = typeof item?.igdbImageId === 'string' ? item.igdbImageId : undefined
		const cid = blobCid(item?.blob)
		const mimeType = (item?.blob as { mimeType?: unknown } | undefined)?.mimeType
		const mediaType = typeof item?.mediaType === 'string' ? item.mediaType : 'artwork'
		if (imageId && cid) refs.push({ imageId, cid, mimeType: typeof mimeType === 'string' ? mimeType : 'image/jpeg', mediaType })
	}
	return refs
}

/**
 * The CIDs of the media in `changes` whose blobs are recorded as mirrored to
 * the catalogue repo at that very CID, sorted and unique. Part of a
 * correction's rkey: a media change that auto-accept gave up on (its blobs
 * were missing) comes back with the same base and the same changes, and would
 * otherwise reuse the old rkey and be skipped forever. Once mirroring lands
 * its blobs, this list changes and the contribution gets a fresh rkey.
 */
export function mirroredMediaCids(changes: Record<string, unknown>, isImageMirrored?: (imageId: string, cid: string) => boolean): string[] {
	if (!isImageMirrored) return []
	const cids = mediaRefs(changes).filter((r) => isImageMirrored(r.imageId, r.cid)).map((r) => r.cid)
	return [...new Set(cids)].sort()
}

const BLOB_NOT_FOUND = /BlobNotFound|Could not find blob/

function isBlobNotFound(err: unknown): boolean {
	return err instanceof ContributionRequestError && BLOB_NOT_FOUND.test(err.message)
}

/**
 * Create a contribution unless one with the same content already exists. If
 * the contributor's PDS refuses it because a media blob is missing (the ledger
 * was wrong, or the PDS deleted a blob nothing referenced yet), forget those
 * blobs, copy them again and retry once with the same media. If they can't be
 * restored, or the retry is refused too, retry once more without `media`, so
 * the other fields still land and the media follows on a later run.
 */
async function submit(
	contributionClient: ContributionClient,
	params: { contributionType: 'correction' | 'newGame'; changes: Record<string, unknown>; subject?: string; baseCid?: string },
	options: SyncGameOptions,
	mediaRetried = false,
): Promise<ContributionSyncResult> {
	const { contributionType, changes, subject, baseCid } = params
	const rkey = contributionType === 'newGame'
		? contentHashRkey(changes)
		: contentHashRkey(changes, baseCid, mirroredMediaCids(changes, options.isImageMirrored))
	const existingRecord = await contributionClient.getRecord(CONTRIBUTION_COLLECTION, rkey)
	if (existingRecord) {
		// That contribution already references these blobs, so they are kept.
		if (changes.media !== undefined) options.onContributorBlobsReferenced?.(mediaRefs(changes))
		return { action: 'skipped' }
	}

	try {
		const result = await contributionClient.createContribution({ contributionType, changes, rkey, ...(subject ? { subject } : {}) })
		if (changes.media !== undefined) options.onContributorBlobsReferenced?.(mediaRefs(changes))
		return { action: 'created', contributionType, uri: result.uri }
	} catch (err) {
		// The without-media retry can't come back here, and the with-media one
		// only once: at most two retries.
		if (!isBlobNotFound(err) || changes.media === undefined) throw err
		const refs = mediaRefs(changes)
		options.onMissingContributorBlobs?.(refs.map((r) => r.imageId))
		const complete = Array.isArray(changes.media) && refs.length === changes.media.length
		if (!mediaRetried && complete && options.restoreContributorBlobs) {
			let restored = false
			try {
				restored = await options.restoreContributorBlobs(refs)
			} catch (restoreErr) {
				console.warn(`  [contributions] Could not restore media blobs:`, (restoreErr as Error).message)
			}
			if (restored) {
				console.warn(`  [contributions] Contributor repo was missing media blobs (${refs.length} images); copied them again, retrying with media`)
				return submit(contributionClient, params, options, true)
			}
		}
		console.warn(`  [contributions] Contributor repo is missing media blobs (${refs.length} images); retrying without media`)
		const { media: _media, ...rest } = changes
		if (Object.keys(rest).length === 0) {
			return { action: 'skipped' }
		}
		return submit(contributionClient, { ...params, changes: rest }, options, true)
	}
}

/**
 * Sync a single IGDB game via the contribution path.
 *
 * 1. Look up the game by IGDB ID via HappyView (resolves redirects).
 * 2. If not found → newGame contribution with the contributable fields.
 * 3. If found → diff the mapped record against the stored game record (not
 *    getGame's hydrated view) and create a correction if anything changed.
 */
export async function syncGameViaContribution(
	igdbId: string,
	mappedRecord: Record<string, unknown>,
	contributionClient: ContributionClient,
	options: SyncGameOptions = {},
): Promise<ContributionSyncResult> {
	const existing = await contributionClient.getGameByIgdbId(igdbId)

	if (!existing) {
		const changes = pickContributableFields(mappedRecord)
		// acceptContribution and auto-accept refuse a newGame without one, and
		// it would sit pending for good. The caller records the game as held,
		// and a later run proposes it once its game_type maps.
		if (!changes.applicationType) {
			console.warn(`  [contributions] IGDB ${igdbId} has no applicationType; holding it instead of proposing it as a new game`)
			return { action: 'held' }
		}
		return submit(contributionClient, { contributionType: 'newGame', changes }, options)
	}

	// Raw reads only go to repos we trust (the catalogue and our own). A game
	// in anyone else's repo is owner-held, which auto-accept skips anyway, so
	// there is nothing to diff it for.
	const ownerDid = existing.uri.match(/^at:\/\/([^/]+)\//)?.[1]
	if (!ownerDid || !contributionClient.canReadRepo(ownerDid)) {
		return { action: 'skipped' }
	}

	const stored = await contributionClient.getGameRecord(existing.uri)
	if (!stored) {
		throw new RecordUnreadableError(existing.uri, igdbId)
	}

	const changes = diffGameRecord(mappedRecord, stored.value)

	if (Object.keys(changes).length === 0) {
		return { action: 'skipped' }
	}

	// The rkey covers the base record and the mirror state of the media: the
	// same diff against a changed base, or with its blobs newly in the
	// catalogue, is a new contribution, not a duplicate of the old one.
	const result = await submit(contributionClient, { contributionType: 'correction', changes, subject: existing.uri, baseCid: stored.cid }, options)

	if (existing.redirectedFrom && result.action === 'created') {
		console.log(`  [contributions] Game ${igdbId} was migrated from ${existing.redirectedFrom} → ${existing.uri}`)
	}

	return result
}
