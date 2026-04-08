/**
 * Pipeline phase: Sync new companies
 *
 * After games are synced, checks for any companies referenced in
 * involved_companies that don't have org profiles yet, and creates them.
 *
 * Uses the IGDB companies endpoint to fetch full company details for
 * any missing profiles, then batch-creates them on the PDS.
 */

import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { IGDBCompany, IGDBWebsite } from '../igdb/types.js'
import { IGDB_WEBSITE_CATEGORY } from '../igdb/types.js'
import { errorLabel } from '../helpers.js'

const COLLECTION_PROFILE = 'games.gamesgamesgamesgames.org.profile'
const BATCH_SIZE = 50

const COMPANY_STATUS_MAP: Record<number, string> = {
	0: 'active',
	1: 'inactive',
	2: 'merged',
	3: 'acquired',
	4: 'defunct',
}

function mapCompany(
	company: IGDBCompany,
	state: StateManager,
): Record<string, unknown> {
	const record: Record<string, unknown> = {
		displayName: company.name,
		externalIds: { igdb: String(company.id) },
		createdAt: new Date().toISOString(),
	}

	if (company.description) {
		record.description = company.description.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
	}

	if (company.country != null) {
		record.country = String(company.country)
	}

	if (company.start_date) {
		record.foundedAt = new Date(company.start_date * 1000).toISOString()
	}

	const statusId = typeof company.status === 'object' ? company.status.id : company.status
	if (statusId != null) {
		const status = COMPANY_STATUS_MAP[statusId]
		if (status) record.status = status
	}

	const parentId = typeof company.parent === 'object' ? company.parent.id : company.parent
	if (parentId != null) {
		const parentUri = state.getEntity('org', parentId)
		if (parentUri) record.parent = parentUri
	}

	if (company.websites && company.websites.length > 0) {
		const websites = mapWebsites(company.websites)
		if (websites.length > 0) record.websites = websites
	}

	return record
}

function mapWebsites(websites: IGDBWebsite[]): Array<Record<string, unknown>> {
	return websites
		.filter((w) => w.url)
		.map((w) => {
			const wcat = w.type ?? w.category
			const type = wcat != null ? IGDB_WEBSITE_CATEGORY[wcat] : 'other'
			return { url: w.url!, type: type ?? 'other' }
		})
}

/**
 * Check for companies that don't have org profiles yet and create them.
 *
 * Uses the IGDB companies endpoint with updated_at filtering to find
 * new or updated companies, then creates profiles for any that are missing.
 */
export async function syncNewCompanies(
	igdb: IGDBClient,
	atproto: AtprotoClient,
	state: StateManager,
): Promise<void> {
	const syncStartedAt = Math.floor(Date.now() / 1000)
	const lastSyncAt = state.getLastSyncAt('org')

	let where = ''
	if (lastSyncAt) {
		where = `where updated_at > ${lastSyncAt};`
		console.log(`[sync] org: incremental sync (changes since ${new Date(lastSyncAt * 1000).toISOString()})`)
	} else {
		console.log('[sync] org: full scan (no previous sync timestamp)')
	}

	const fields = [
		'fields name, slug, description, country, start_date, start_date_category,',
		'logo.image_id, logo.width, logo.height,',
		'websites.url, websites.category, websites.type,',
		'parent, status, updated_at;',
	].join(' ')

	let created = 0
	let skipped = 0
	let failed = 0
	let pending: Array<{ company: IGDBCompany; record: Record<string, unknown> }> = []

	const flushCreates = async () => {
		if (pending.length === 0) return
		try {
			const results = await atproto.applyCreates(
				pending.map((p) => ({ collection: COLLECTION_PROFILE, record: p.record })),
			)
			for (let i = 0; i < pending.length; i++) {
				const entry = pending[i]!
				const result = results[i]
				if (result) {
					state.setEntity('org', entry.company.id, result.uri)
					created++
				}
			}
			console.log(`  [sync] org: created ${pending.length} profiles (${created} total)`)
		} catch (err) {
			console.warn(`  [!] Batch failed (${(err as Error).message}), falling back to individual creates`)
			for (const entry of pending) {
				try {
					const result = await atproto.createRecord(COLLECTION_PROFILE, entry.record)
					state.setEntity('org', entry.company.id, result.uri)
					created++
				} catch (innerErr) {
					console.error(`  [!] ${errorLabel(innerErr)} ${entry.company.name}: ${(innerErr as Error).message}`)
					failed++
				}
			}
		}
		pending = []
	}

	for await (const { items } of igdb.paginate<IGDBCompany>('companies', fields, 0, where)) {
		for (const company of items) {
			if (state.hasEntity('org', company.id)) {
				skipped++
				continue
			}

			const record = mapCompany(company, state)
			pending.push({ company, record })
			if (pending.length >= BATCH_SIZE) {
				await flushCreates()
			}
		}
	}

	await flushCreates()
	state.setLastSyncAt('org', syncStartedAt)
	console.log(`[sync] org: done (created=${created}, skipped=${skipped}, failed=${failed})`)
}
