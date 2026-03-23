/**
 * Pipeline phase: Organization Credits (Involved Companies)
 *
 * Creates org.credit records linking companies to games with their roles.
 * This phase re-queries IGDB for involved_companies since we need the
 * game AT-URIs that were created in the games phase.
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBInvolvedCompany } from '../igdb/types.js'
import { mapOrgCredit } from '../atproto/mapping.js'
import { errorLabel } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'

const COLLECTION = 'games.gamesgamesgamesgames.org.credit'

export async function scrapeCredits(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('credits')) {
		console.log('[pipeline] Credits already done, skipping')
		return
	}

	console.log('[pipeline] Starting org credits...')
	const offset = state.getOffset('credits')

	const fields = [
		'fields company.id, company.name, company.slug,',
		'game, developer, publisher, porting, supporting;',
	].join(' ')

	let totalProcessed = 0

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBInvolvedCompany>(
		'involved_companies',
		fields,
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const ic of items) {
			if (state.hasEntity('orgCredit', ic.id)) {
				totalProcessed++
				continue
			}

			// Look up the game's atproto URI
			const gameId = typeof ic.game === 'number' ? ic.game : undefined
			if (!gameId) continue

			const gameUri = state.getEntity('game', gameId)
			if (!gameUri) {
				// Game wasn't created (maybe it failed) — skip
				continue
			}

			try {
				// We need the CID too for strongRef — use a placeholder since
				// we don't store CIDs in state. The org.credit record uses
				// strongRef which needs both uri and cid.
				// For now we'll store a dummy CID; a production version would
				// need to resolve this or store CIDs in state.
				const record = mapOrgCredit(ic, gameUri, 'bafyreig6') // placeholder CID

				if (!record) continue

				batch.push({ id: ic.id, entityType: 'orgCredit', record, collection: COLLECTION, name: `Credit ${ic.id}` })
				totalProcessed++

				if (totalProcessed % 500 === 0) {
					console.log(`  [+] Credits processed: ${totalProcessed}`)
				}

				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed credit ${ic.id}:`, (err as Error).message)
				state.addFailure('orgCredit', ic.id)
			}
		}

		await flushBatch(batch, atproto, state)
		state.setOffset('credits', currentOffset + items.length)
		state.save()
	}

	// Retry failed credits
	const creditFailures = state.getFailures('orgCredit')
	if (creditFailures.length > 0) {
		console.log(`[pipeline] Retrying ${creditFailures.length} failed credits...`)
		const ids = creditFailures.join(',')
		const retryItems = await igdb.query<IGDBInvolvedCompany>(
			'involved_companies',
			`${fields} where id = (${ids}); limit 500;`,
		)
		const batch: PendingRecord[] = []
		for (const ic of retryItems) {
			if (state.hasEntity('orgCredit', ic.id)) {
				state.removeFailure('orgCredit', ic.id)
				continue
			}
			const gameId = typeof ic.game === 'number' ? ic.game : undefined
			if (!gameId) continue
			const gameUri = state.getEntity('game', gameId)
			if (!gameUri) continue
			try {
				const record = mapOrgCredit(ic, gameUri, 'bafyreig6')
				if (!record) continue
				batch.push({ id: ic.id, entityType: 'orgCredit', record, collection: COLLECTION, name: `Retry succeeded: credit ${ic.id}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					for (const entry of batch) {
						if (state.hasEntity('orgCredit', entry.id)) state.removeFailure('orgCredit', entry.id)
					}
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Retry failed: credit ${ic.id}:`, (err as Error).message)
			}
		}
		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			if (state.hasEntity('orgCredit', entry.id)) state.removeFailure('orgCredit', entry.id)
		}
		state.save()
	}

	state.markPhaseDone('credits')
	console.log(`[pipeline] Org credits done (${totalProcessed} total)`)
}
