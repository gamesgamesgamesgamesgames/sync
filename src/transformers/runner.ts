import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { Transformer, IGDBEndpoint, TransformerContext } from './types.js'
import { ENDPOINT_CONFIG } from './types.js'
import { transformers } from './index.js'

const UPDATE_BATCH_SIZE = 20

export async function runTransformers(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	const ctx: TransformerContext = { igdb, atproto, state }

	// Graceful shutdown
	let shuttingDown = false
	const shutdown = () => {
		if (shuttingDown) return
		shuttingDown = true
		console.log('\n[shutdown] Graceful shutdown requested, finishing current page...')
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	// Filter to pending transformers
	const pending = transformers.filter((t) => {
		const config = ENDPOINT_CONFIG[t.endpoint]
		return !state.isTransformerDone(t.name, config.entityType)
	})

	if (pending.length === 0) {
		console.log('[transformers] All transformers are up to date.')
		return
	}

	console.log(`[transformers] ${pending.length} pending transformer(s): ${pending.map((t) => t.name).join(', ')}`)

	// Group by endpoint
	const groups = new Map<IGDBEndpoint, Transformer<any>[]>()
	for (const t of pending) {
		const group = groups.get(t.endpoint) ?? []
		group.push(t)
		groups.set(t.endpoint, group)
	}

	for (const [endpoint, group] of groups) {
		if (shuttingDown) break

		const config = ENDPOINT_CONFIG[endpoint]
		console.log(`\n[transformers] Processing endpoint: ${endpoint} (${group.length} transformer(s))`)

		// Merge igdbFields across all transformers in this group
		const allFields = new Set<string>()
		for (const t of group) {
			for (const field of t.igdbFields.split(',')) {
				allFields.add(field.trim())
			}
		}
		// Always include id
		allFields.add('id')
		const fieldsClause = `fields ${[...allFields].join(', ')};`

		// Find minimum offset across all transformers in this group
		const offsets = group.map((t) => state.getTransformerOffset(t.name, config.entityType))
		const startOffset = Math.min(...offsets)

		console.log(`[transformers] Fields: ${fieldsClause}`)
		console.log(`[transformers] Starting from offset ${startOffset}`)

		const stats = { processed: 0, updated: 0, skipped: 0, notInState: 0 }
		const pages = igdb.paginate<Record<string, unknown>>(endpoint, fieldsClause, startOffset)

		for await (const { items, offset } of pages) {
			if (shuttingDown) break

			const updateBatch: Array<{ collection: string; rkey: string; record: Record<string, unknown> }> = []

			for (const igdbItem of items) {
				stats.processed++
				const igdbId = igdbItem.id as number

				// Determine which transformers are active for this item
				// (their saved offset is <= current page offset)
				const activeTransformers = group.filter((t) => {
					const tOffset = state.getTransformerOffset(t.name, config.entityType)
					return tOffset <= offset
				})

				if (activeTransformers.length === 0) continue

				// Look up AT-URI in state
				const uri = state.getEntity(config.entityType, igdbId)
				if (!uri) {
					stats.notInState++
					continue
				}

				// Fetch PDS record
				const rkey = uri.split('/').pop()!
				let existing: Record<string, unknown> | null
				try {
					existing = await atproto.getRecord(config.collection, rkey)
				} catch (err) {
					console.error(`[error] Failed to fetch record for ${endpoint} IGDB ${igdbId}:`, (err as Error).message)
					continue
				}

				if (!existing) {
					stats.skipped++
					continue
				}

				// Strip $type, run transformers on a mutable copy
				const { $type: _$type, ...record } = existing
				let modified = false

				for (const t of activeTransformers) {
					const result = await t.transform(record, igdbItem, ctx)
					if (result) modified = true
				}

				if (modified) {
					updateBatch.push({ collection: config.collection, rkey, record })
				}
			}

			// Flush update batch
			if (updateBatch.length > 0) {
				for (let i = 0; i < updateBatch.length; i += UPDATE_BATCH_SIZE) {
					const chunk = updateBatch.slice(i, i + UPDATE_BATCH_SIZE)
					try {
						await atproto.applyUpdates(chunk)
						stats.updated += chunk.length
					} catch {
						console.warn('[warn] Batch update failed, falling back to individual writes')
						for (const write of chunk) {
							try {
								await atproto.putRecord(config.collection, write.rkey, write.record)
								stats.updated++
							} catch (err) {
								console.error(`[error] putRecord failed for rkey ${write.rkey}:`, (err as Error).message)
							}
						}
					}
				}
			}

			// Update each transformer's offset after this page
			const nextOffset = offset + items.length
			for (const t of group) {
				const tOffset = state.getTransformerOffset(t.name, config.entityType)
				if (tOffset <= offset) {
					state.setTransformerOffset(t.name, config.entityType, nextOffset)
				}
			}

			console.log(
				`[transformers] ${endpoint} offset=${nextOffset} | processed=${stats.processed} updated=${stats.updated} skipped=${stats.skipped} notInState=${stats.notInState}`,
			)
		}

		// Mark transformers done (only if we weren't interrupted)
		if (!shuttingDown) {
			for (const t of group) {
				state.markTransformerDone(t.name, config.entityType)
				console.log(`[transformers] Marked "${t.name}" as done for ${config.entityType}`)
			}
		}

		console.log(
			`[transformers] ${endpoint} complete: processed=${stats.processed} updated=${stats.updated} skipped=${stats.skipped} notInState=${stats.notInState}`,
		)
	}

	if (shuttingDown) {
		console.log('[transformers] Shutdown complete. Progress saved — will resume on next run.')
	} else {
		console.log('\n[transformers] All transformers complete.')
	}
}
