/**
 * Pipeline phase: Platform Families and Platforms
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBPlatformFamily, IGDBPlatform } from '../igdb/types.js'
import { mapPlatformFamily, mapPlatform } from '../atproto/mapping.js'
import { errorLabel } from '../helpers.js'
import { flushBatch, type PendingRecord } from './batch.js'

const COLLECTION_PLATFORM_FAMILY = 'games.gamesgamesgamesgames.platformFamily'
const COLLECTION_PLATFORM = 'games.gamesgamesgamesgames.platform'

export async function scrapePlatformFamilies(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('platformFamilies')) {
		console.log('[pipeline] Platform families already done, skipping')
		return
	}

	console.log('[pipeline] Starting platform families...')
	const offset = state.getOffset('platformFamilies')

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBPlatformFamily>(
		'platform_families',
		'fields name, slug;',
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const family of items) {
			if (state.hasEntity('platformFamily', family.id)) continue

			try {
				const record = mapPlatformFamily(family)
				batch.push({ id: family.id, entityType: 'platformFamily', record, collection: COLLECTION_PLATFORM_FAMILY, name: `Platform family: ${family.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${family.name}:`, (err as Error).message)
				state.addFailure('platformFamily', family.id)
			}
		}

		await flushBatch(batch, atproto, state)
		state.setOffset('platformFamilies', currentOffset + items.length)
		state.save()
	}

	// Retry failed platform families
	const familyFailures = state.getFailures('platformFamily')
	if (familyFailures.length > 0) {
		console.log(`[pipeline] Retrying ${familyFailures.length} failed platform families...`)
		const ids = familyFailures.join(',')
		const retryItems = await igdb.query<IGDBPlatformFamily>(
			'platform_families',
			`fields name, slug; where id = (${ids}); limit 500;`,
		)
		const batch: PendingRecord[] = []
		for (const family of retryItems) {
			if (state.hasEntity('platformFamily', family.id)) {
				state.removeFailure('platformFamily', family.id)
				continue
			}
			try {
				const record = mapPlatformFamily(family)
				batch.push({ id: family.id, entityType: 'platformFamily', record, collection: COLLECTION_PLATFORM_FAMILY, name: `Retry succeeded: ${family.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					for (const entry of batch) {
						if (state.hasEntity('platformFamily', entry.id)) state.removeFailure('platformFamily', entry.id)
					}
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Retry failed: ${family.name}:`, (err as Error).message)
			}
		}
		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			if (state.hasEntity('platformFamily', entry.id)) state.removeFailure('platformFamily', entry.id)
		}
		state.save()
	}

	state.markPhaseDone('platformFamilies')
	console.log('[pipeline] Platform families done')
}

export async function scrapePlatforms(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	if (state.isPhaseDone('platforms')) {
		console.log('[pipeline] Platforms already done, skipping')
		return
	}

	console.log('[pipeline] Starting platforms...')
	const offset = state.getOffset('platforms')

	const fields = [
		'fields name, abbreviation, alternative_name, category, generation,',
		'summary, slug, platform_family,',
		'platform_logo.image_id, platform_logo.width, platform_logo.height,',
		'versions.name, versions.summary, versions.cpu, versions.graphics,',
		'versions.memory, versions.storage, versions.connectivity,',
		'versions.os, versions.output, versions.resolutions,',
		'websites.type, websites.url;',
	].join(' ')

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBPlatform>(
		'platforms',
		fields,
		offset,
	)) {
		const batch: PendingRecord[] = []

		for (const platform of items) {
			if (state.hasEntity('platform', platform.id)) continue

			try {
				const record = await mapPlatform(platform, igdb, atproto, state)
				batch.push({ id: platform.id, entityType: 'platform', record, collection: COLLECTION_PLATFORM, name: `Platform: ${platform.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Failed: ${platform.name}:`, (err as Error).message)
				state.addFailure('platform', platform.id)
			}
		}

		await flushBatch(batch, atproto, state)
		state.setOffset('platforms', currentOffset + items.length)
		state.save()
	}

	// Retry failed platforms
	const platformFailures = state.getFailures('platform')
	if (platformFailures.length > 0) {
		console.log(`[pipeline] Retrying ${platformFailures.length} failed platforms...`)
		const ids = platformFailures.join(',')
		const retryItems = await igdb.query<IGDBPlatform>(
			'platforms',
			`${fields} where id = (${ids}); limit 500;`,
		)
		const batch: PendingRecord[] = []
		for (const platform of retryItems) {
			if (state.hasEntity('platform', platform.id)) {
				state.removeFailure('platform', platform.id)
				continue
			}
			try {
				const record = await mapPlatform(platform, igdb, atproto, state)
				batch.push({ id: platform.id, entityType: 'platform', record, collection: COLLECTION_PLATFORM, name: `Retry succeeded: ${platform.name}` })
				if (batch.length >= 200) {
					await flushBatch(batch, atproto, state)
					for (const entry of batch) {
						if (state.hasEntity('platform', entry.id)) state.removeFailure('platform', entry.id)
					}
					batch.length = 0
				}
			} catch (err) {
				console.error(`  [!] ${errorLabel(err)} Retry failed: ${platform.name}:`, (err as Error).message)
			}
		}
		await flushBatch(batch, atproto, state)
		for (const entry of batch) {
			if (state.hasEntity('platform', entry.id)) state.removeFailure('platform', entry.id)
		}
		state.save()
	}

	state.markPhaseDone('platforms')
	console.log('[pipeline] Platforms done')
}
