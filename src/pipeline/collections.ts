/**
 * Pipeline phase: Collections and Franchises
 *
 * Both IGDB Collections and Franchises map to the lexicon's
 * `collection` record, differentiated by the `type` field.
 *
 * IGDB IDs are stored with a prefix to avoid collisions:
 *   - Collections: "c_{id}"
 *   - Franchises: "f_{id}"
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBCollection, IGDBFranchise } from '../igdb/types.js'
import { mapCollection, mapFranchise } from '../atproto/mapping.js'
import { errorLabel } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'

const COLLECTION_NSID = 'games.gamesgamesgamesgames.collection'

export async function scrapeCollections(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('collections')) {
		console.log('[pipeline] Collections already done, skipping')
		return
	}

	console.log('[pipeline] Starting collections...')

	// Phase 1: IGDB Collections → type: "series"
	await scrapeIGDBCollections(igdb, atproto, state)

	// Phase 2: IGDB Franchises → type: "franchise"
	await scrapeIGDBFranchises(igdb, atproto, state)

	// Retry failed collections and franchises
	const collectionFailures = state.getFailures('collection')
	if (collectionFailures.length > 0) {
		// Separate c_ (collection) and f_ (franchise) failures
		const collectionIds = collectionFailures
			.filter((k) => k.startsWith('c_'))
			.map((k) => k.slice(2))
		const franchiseIds = collectionFailures
			.filter((k) => k.startsWith('f_'))
			.map((k) => k.slice(2))

		if (collectionIds.length > 0) {
			console.log(`[pipeline] Retrying ${collectionIds.length} failed collections...`)
			const ids = collectionIds.join(',')
			const retryItems = await igdb.query<IGDBCollection>(
				'collections',
				`fields name, slug, games, url; where id = (${ids}); limit 500;`,
			)
			const batch: PendingRecord[] = []
			for (const collection of retryItems) {
				const key = `c_${collection.id}`
				if (state.hasEntity('collection', key)) {
					state.removeFailure('collection', key)
					continue
				}
				try {
					const record = mapCollection(collection, state)
					batch.push({ id: key, entityType: 'collection', record, collection: COLLECTION_NSID, name: `Retry succeeded: ${collection.name}` })
					if (batch.length >= 200) {
						await flushBatch(batch, atproto, state)
						for (const entry of batch) {
							if (state.hasEntity('collection', entry.id)) state.removeFailure('collection', entry.id)
						}
						batch.length = 0
					}
				} catch (err) {
					console.error(`  [!] ${errorLabel(err)} Retry failed: ${collection.name}:`, (err as Error).message)
				}
			}
			await flushBatch(batch, atproto, state)
			for (const entry of batch) {
				if (state.hasEntity('collection', entry.id)) state.removeFailure('collection', entry.id)
			}
		}

		if (franchiseIds.length > 0) {
			console.log(`[pipeline] Retrying ${franchiseIds.length} failed franchises...`)
			const ids = franchiseIds.join(',')
			const retryItems = await igdb.query<IGDBFranchise>(
				'franchises',
				`fields name, slug, games, url; where id = (${ids}); limit 500;`,
			)
			const batch: PendingRecord[] = []
			for (const franchise of retryItems) {
				const key = `f_${franchise.id}`
				if (state.hasEntity('collection', key)) {
					state.removeFailure('collection', key)
					continue
				}
				try {
					const record = mapFranchise(franchise, state)
					batch.push({ id: key, entityType: 'collection', record, collection: COLLECTION_NSID, name: `Retry succeeded: ${franchise.name}` })
					if (batch.length >= 200) {
						await flushBatch(batch, atproto, state)
						for (const entry of batch) {
							if (state.hasEntity('collection', entry.id)) state.removeFailure('collection', entry.id)
						}
						batch.length = 0
					}
				} catch (err) {
					console.error(`  [!] ${errorLabel(err)} Retry failed: ${franchise.name}:`, (err as Error).message)
				}
			}
			await flushBatch(batch, atproto, state)
			for (const entry of batch) {
				if (state.hasEntity('collection', entry.id)) state.removeFailure('collection', entry.id)
			}
		}

		state.save()
	}

	state.markPhaseDone('collections')
	console.log('[pipeline] Collections done')
}

async function scrapeIGDBCollections(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	console.log('[pipeline]   Scraping IGDB collections...')

	// We use the collections offset for IGDB collections.
	// Once collections are done, we reset offset for franchises.
	const offset = state.getOffset('collections')

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBCollection>(
		'collections',
		'fields name, slug, games, url;',
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const collection of items) {
			const key = `c_${collection.id}`
			if (state.hasEntity('collection', key)) continue

			try {
				const record = mapCollection(collection, state)
				batch.push({ id: key, entityType: 'collection', record, collection: COLLECTION_NSID, name: `Collection: ${collection.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${collection.name}:`, (err as Error).message)
				state.addFailure('collection', key)
			}
		}

		await flushBatch(batch, atproto, state)
		state.setOffset('collections', currentOffset + items.length)
		state.save()
	}
}

async function scrapeIGDBFranchises(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	console.log('[pipeline]   Scraping IGDB franchises...')

	// For franchises, we paginate separately.
	// We'll track franchise offset by checking if franchise entities exist already.
	let offset = 0

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBFranchise>(
		'franchises',
		'fields name, slug, games, url;',
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const franchise of items) {
			const key = `f_${franchise.id}`
			if (state.hasEntity('collection', key)) continue

			try {
				const record = mapFranchise(franchise, state)
				batch.push({ id: key, entityType: 'collection', record, collection: COLLECTION_NSID, name: `Franchise: ${franchise.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${franchise.name}:`, (err as Error).message)
				state.addFailure('collection', key)
			}
		}

		await flushBatch(batch, atproto, state)
		state.save()
	}
}
