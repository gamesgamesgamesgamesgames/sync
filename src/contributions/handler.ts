import { diffGameRecord, pickContributableFields } from './diff.js'
import { contentHashRkey } from './hash.js'
import { ContributionRequestError, type ContributionClient } from './client.js'

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
	/**
	 * Called with the IGDB image ids of a contribution's media when the
	 * contributor's PDS refused it for a missing blob, so the ledger stops
	 * claiming those blobs are there and a later run copies them again.
	 */
	onMissingContributorBlobs?: (imageIds: string[]) => void
}

const BLOB_NOT_FOUND = /BlobNotFound|Could not find blob/

function isBlobNotFound(err: unknown): boolean {
	return err instanceof ContributionRequestError && BLOB_NOT_FOUND.test(err.message)
}

function mediaImageIds(changes: Record<string, unknown>): string[] {
	if (!Array.isArray(changes.media)) return []
	return (changes.media as Array<{ igdbImageId?: unknown }>)
		.map((m) => m?.igdbImageId)
		.filter((id): id is string => typeof id === 'string')
}

/**
 * Create a contribution unless one with the same content already exists. If
 * the contributor's PDS refuses it because a media blob is missing (the ledger
 * was wrong), forget those blobs and retry once without `media`, so the other
 * fields still land and the media follows on a later run.
 */
async function submit(
	contributionClient: ContributionClient,
	params: { contributionType: 'correction' | 'newGame'; changes: Record<string, unknown>; subject?: string; baseCid?: string },
	options: SyncGameOptions,
): Promise<ContributionSyncResult> {
	const { contributionType, changes, subject, baseCid } = params
	const rkey = contributionType === 'newGame' ? contentHashRkey(changes) : contentHashRkey(changes, baseCid)
	const existingRecord = await contributionClient.getRecord(CONTRIBUTION_COLLECTION, rkey)
	if (existingRecord) {
		return { action: 'skipped' }
	}

	try {
		const result = await contributionClient.createContribution({ contributionType, changes, rkey, ...(subject ? { subject } : {}) })
		return { action: 'created', contributionType, uri: result.uri }
	} catch (err) {
		// The retry has no media, so it can't come back here: at most one retry.
		if (!isBlobNotFound(err) || changes.media === undefined) throw err
		const imageIds = mediaImageIds(changes)
		options.onMissingContributorBlobs?.(imageIds)
		console.warn(`  [contributions] Contributor repo is missing media blobs (${imageIds.length} images); retrying without media`)
		const { media: _media, ...rest } = changes
		if (Object.keys(rest).length === 0) {
			return { action: 'skipped' }
		}
		return submit(contributionClient, { ...params, changes: rest }, options)
	}
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

	// The rkey covers the base record too: the same diff against a changed
	// base (media given up at the deferral cap, IGDB restoring a reverted
	// field) is a new contribution, not a duplicate of the old one.
	const result = await submit(contributionClient, { contributionType: 'correction', changes, subject: existing.uri, baseCid: stored.cid }, options)

	if (existing.redirectedFrom && result.action === 'created') {
		console.log(`  [contributions] Game ${igdbId} was migrated from ${existing.redirectedFrom} → ${existing.uri}`)
	}

	return result
}
