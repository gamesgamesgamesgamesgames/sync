/**
 * Pipeline phase: Game Engines
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBGameEngine } from '../igdb/types.js'
import { mapEngine } from '../atproto/mapping.js'
import { errorLabel } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'

const COLLECTION = 'games.gamesgamesgamesgames.engine'

export async function scrapeEngines(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('engines')) {
		console.log('[pipeline] Engines already done, skipping')
		return
	}

	console.log('[pipeline] Starting engines...')
	const offset = state.getOffset('engines')

	const fields = [
		'fields name, description, slug, url,',
		'logo.image_id, logo.width, logo.height,',
		'platforms, companies;',
	].join(' ')

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBGameEngine>(
		'game_engines',
		fields,
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const engine of items) {
			if (state.hasEntity('engine', engine.id)) continue

			try {
				const record = await mapEngine(engine, igdb, atproto, state)
				batch.push({ id: engine.id, entityType: 'engine', record, collection: COLLECTION, name: `Engine: ${engine.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${engine.name}:`, (err as Error).message)
				state.addFailure('engine', engine.id)
			}
		}

		await flushBatch(batch, atproto, state)
		state.setOffset('engines', currentOffset + items.length)
		state.save()
	}

	// Retry failed engines
	const engineFailures = state.getFailures('engine')
	if (engineFailures.length > 0) {
		console.log(`[pipeline] Retrying ${engineFailures.length} failed engines...`)
		const ids = engineFailures.join(',')
		const retryItems = await igdb.query<IGDBGameEngine>(
			'game_engines',
			`${fields} where id = (${ids}); limit 500;`,
		)
		const batch: PendingRecord[] = []
		for (const engine of retryItems) {
			if (state.hasEntity('engine', engine.id)) {
				state.removeFailure('engine', engine.id)
				continue
			}
			try {
				const record = await mapEngine(engine, igdb, atproto, state)
				batch.push({ id: engine.id, entityType: 'engine', record, collection: COLLECTION, name: `Retry succeeded: ${engine.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					for (const entry of batch) {
						if (state.hasEntity('engine', entry.id)) state.removeFailure('engine', entry.id)
					}
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Retry failed: ${engine.name}:`, (err as Error).message)
			}
		}
		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			if (state.hasEntity('engine', entry.id)) state.removeFailure('engine', entry.id)
		}
		state.save()
	}

	state.markPhaseDone('engines')
	console.log('[pipeline] Engines done')
}
