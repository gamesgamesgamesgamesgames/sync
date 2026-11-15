import type { ContributionClient } from './client.js'
import { diffGameRecord, pickContributableFields } from './diff.js'
import { contentHashRkey } from './hash.js'

/**
 * getGame found a game but its stored record could not be read. That is
 * never the game's fault: it means the read path is broken, so it counts as
 * systemic and is never charged to the game.
 */
export class RecordUnreadableError extends Error {
	constructor(readonly uri: string, readonly igdbId: string) {
		super(`getGame found ${uri} for IGDB ${igdbId}, but its record could not be read`)
		this.name = 'RecordUnreadableError'
	}
}

const CONTRIBUTION_COLLECTION = 'games.gamesgamesgamesgames.contribution'

export interface ContributionSyncResult {
	action: 'created' | 'skipped'
	contributionType?: 'correction' | 'newGame'
	uri?: string
}

/**
 * Sync a single IGDB game via the contribution path.
 *
 * 1. Look up the game by IGDB ID via HappyView (resolves redirects).
 * 2. If not found → newGame contribution with the contributable fields.
 * 3. If found → diff the mapped record against the stored game record (not
 *    getGame's hydrated view) and create a correction if anything changed.
 */
export interface SyncGameOptions {
	/** Whether an IGDB image's blob is known to be in the catalogue repo (blob_mirrors). */
	isImageMirrored?: (imageId: string) => boolean
}

/**
 * The CIDs of the media in `changes` whose blobs are known to be mirrored to
 * the catalogue repo, sorted. Part of a correction's rkey: a media change that
 * auto-accept gave up on (its blobs were missing) comes back with the same
 * base and the same changes, and would otherwise reuse the old rkey and be
 * skipped forever. Once mirroring lands its blobs, this list changes and the
 * contribution gets a fresh rkey.
 */
export function mirroredMediaCids(changes: Record<string, unknown>, isImageMirrored?: (imageId: string) => boolean): string[] {
	if (!isImageMirrored || !Array.isArray(changes.media)) return []
	const cids: string[] = []
	for (const item of changes.media as Array<Record<string, unknown>>) {
		const imageId = typeof item?.igdbImageId === 'string' ? item.igdbImageId : undefined
		const cid = (item?.blob as { ref?: { $link?: unknown } } | undefined)?.ref?.$link
		if (imageId && typeof cid === 'string' && isImageMirrored(imageId)) cids.push(cid)
	}
	return [...new Set(cids)].sort()
}

export async function syncGameViaContribution(
	igdbId: string,
	mappedRecord: Record<string, unknown>,
	contributionClient: ContributionClient,
	options: SyncGameOptions = {},
): Promise<ContributionSyncResult> {
	const existing = await contributionClient.getGameByIgdbId(igdbId)

	if (!existing) {
		const changes = pickContributableFields(mappedRecord)
		const rkey = contentHashRkey(changes)
		const existingRecord = await contributionClient.getRecord(CONTRIBUTION_COLLECTION, rkey)
		if (existingRecord) {
			return { action: 'skipped' }
		}

		const result = await contributionClient.createContribution({
			contributionType: 'newGame',
			changes,
			rkey,
		})
		return { action: 'created', contributionType: 'newGame', uri: result.uri }
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

	// The rkey covers the base record too: the same diff against a changed
	// base (media given up at the deferral cap, IGDB restoring a reverted
	// field) is a new contribution, not a duplicate of the old one.
	const rkey = contentHashRkey(changes, stored.cid, mirroredMediaCids(changes, options.isImageMirrored))
	const existingRecord = await contributionClient.getRecord(CONTRIBUTION_COLLECTION, rkey)
	if (existingRecord) {
		return { action: 'skipped' }
	}

	const subjectUri = existing.uri
	const result = await contributionClient.createContribution({
		contributionType: 'correction',
		changes,
		subject: subjectUri,
		rkey,
	})

	if (existing.redirectedFrom) {
		console.log(`  [contributions] Game ${igdbId} was migrated from ${existing.redirectedFrom} → ${subjectUri}`)
	}

	return { action: 'created', contributionType: 'correction', uri: result.uri }
}
