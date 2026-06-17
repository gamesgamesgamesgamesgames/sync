import type { ContributionClient } from './client.js'
import { diffGameRecord } from './diff.js'

export interface ContributionSyncResult {
	action: 'created' | 'skipped'
	contributionType?: 'correction' | 'newGame'
	uri?: string
}

/**
 * Sync a single IGDB game via the contribution path.
 *
 * 1. Look up the game by IGDB ID via HappyView (resolves redirects).
 * 2. If not found → newGame contribution with full record as changes.
 * 3. If found → diff mapped record against current, create correction if changed.
 */
export async function syncGameViaContribution(
	igdbId: string,
	mappedRecord: Record<string, unknown>,
	contributionClient: ContributionClient,
): Promise<ContributionSyncResult> {
	const existing = await contributionClient.getGameByIgdbId(igdbId)

	if (!existing) {
		const result = await contributionClient.createContribution({
			contributionType: 'newGame',
			changes: mappedRecord,
		})
		return { action: 'created', contributionType: 'newGame', uri: result.uri }
	}

	const changes = diffGameRecord(mappedRecord, existing.record)

	if (Object.keys(changes).length === 0) {
		return { action: 'skipped' }
	}

	const subjectUri = existing.uri
	const result = await contributionClient.createContribution({
		contributionType: 'correction',
		changes,
		subject: subjectUri,
	})

	if (existing.redirectedFrom) {
		console.log(`  [contributions] Game ${igdbId} was migrated from ${existing.redirectedFrom} → ${subjectUri}`)
	}

	return { action: 'created', contributionType: 'correction', uri: result.uri }
}
