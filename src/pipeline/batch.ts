/**
 * Shared batch-create helper for pipeline phases.
 *
 * Buffers records and flushes them via applyWrites (up to 200 per call on Cirrus PDS).
 * Falls back to individual createRecord calls if the batch is rejected.
 */

import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager, EntityType } from '../state.js'
import { errorLabel } from '../helpers.js'

export interface PendingRecord {
	id: string | number
	entityType: EntityType
	record: Record<string, unknown>
	collection: string
	name: string
	slug?: string
}

export async function flushBatch(
	batch: PendingRecord[],
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (batch.length === 0) return

	try {
		const results = await atproto.applyCreates(
			batch.map((r) => ({
				collection: r.collection,
				record: r.record,
			})),
		)

		for (let i = 0; i < batch.length; i++) {
			const entry = batch[i]!
			const result = results[i]
			if (result) {
				state.setEntity(entry.entityType, entry.id, result.uri)
				console.log(`  [+] ${entry.name}`)
			}
		}
	} catch {
		// Batch rejected — fall back to individual creates
		console.warn(`  [!] Batch of ${batch.length} rejected, falling back to individual creates`)
		for (const entry of batch) {
			try {
				const result = await atproto.createRecord(entry.collection, entry.record)
				state.setEntity(entry.entityType, entry.id, result.uri)
				console.log(`  [+] ${entry.name}`)
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${entry.name}:`, (err as Error).message)
				state.addFailure(entry.entityType, entry.id)
			}
		}
	}
}
