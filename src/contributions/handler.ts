import type { ContributionClient } from './client.js'
import { diffGameRecord, pickContributableFields } from './diff.js'
import { contentHashRkey } from './hash.js'

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
export async function syncGameViaContribution(
	igdbId: string,
	mappedRecord: Record<string, unknown>,
	contributionClient: ContributionClient,
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

	const stored = await contributionClient.getGameRecord(existing.uri)
	if (!stored) {
		throw new Error(`getGame found ${existing.uri} for IGDB ${igdbId}, but its record could not be read`)
	}

	const changes = diffGameRecord(mappedRecord, stored.value)

	if (Object.keys(changes).length === 0) {
		return { action: 'skipped' }
	}

	const rkey = contentHashRkey(changes)
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
