/**
 * Backfill org profiles and link existing org credits to them.
 *
 * Phase 1: Fetches all companies from IGDB and creates `org.profile` records
 *          in the central gamesgamesgamesgames.games repo with TID rkeys.
 *          Each profile stores the IGDB company ID in externalIds.igdb so the
 *          profile can be located later.
 *
 * Phase 2: Iterates IGDB involved_companies, looks up the existing `org.credit`
 *          AT URI (from state) and the newly created org profile URI, then
 *          updates the credit record to add the `org` strongRef.
 *
 * Progress is tracked in state.sqlite so the script is fully resumable.
 *
 * Usage:
 *   npx tsx src/backfill-org-profiles.ts
 */

import 'dotenv/config'
import postgres from 'postgres'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { IGDB_WEBSITE_CATEGORY } from './igdb/types.js'
import type {
	IGDBCompany,
	IGDBInvolvedCompany,
	IGDBWebsite,
} from './igdb/types.js'
import { errorLabel } from './helpers.js'

const COLLECTION_PROFILE = 'games.gamesgamesgamesgames.org.profile'
const COLLECTION_CREDIT = 'games.gamesgamesgamesgames.org.credit'
const CREATE_BATCH_SIZE = 50
const UPDATE_BATCH_SIZE = 100

/** IGDB company_status enum → lexicon org status value. */
const COMPANY_STATUS_MAP: Record<number, string> = {
	0: 'active',
	1: 'inactive',
	2: 'merged',
	3: 'acquired',
	4: 'defunct',
}

/** Map an IGDB company → org.profile record. */
function mapCompany(
	company: IGDBCompany,
	state: StateManager,
	orgCids: Map<number, { uri: string; cid: string }>,
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

	// Status
	const statusId = typeof company.status === 'object' ? company.status.id : company.status
	if (statusId != null) {
		const status = COMPANY_STATUS_MAP[statusId]
		if (status) record.status = status
	}

	// Parent — resolve if we've already created the parent profile
	const parentId = typeof company.parent === 'object' ? company.parent.id : company.parent
	if (parentId != null) {
		const parentEntry = orgCids.get(parentId) ?? (() => {
			const uri = state.getEntity('org', parentId)
			return uri ? { uri, cid: '' } : undefined
		})()
		if (parentEntry) {
			record.parent = parentEntry.uri
		}
	}

	// Websites
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

async function main() {
	const {
		TWITCH_CLIENT_ID,
		TWITCH_CLIENT_SECRET,
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
		HAPPYVIEW_DATABASE_URL,
	} = process.env

	if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
		throw new Error('Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET in .env')
	}
	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD in .env')
	}
	if (!HAPPYVIEW_DATABASE_URL) {
		throw new Error('Missing HAPPYVIEW_DATABASE_URL in .env')
	}

	const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
	await igdb.authenticate()

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	const state = new StateManager()
	const db = state.getDb()

	// Progress tables
	db.exec(`
		CREATE TABLE IF NOT EXISTS org_profile_cids (
			igdb_id TEXT PRIMARY KEY,
			at_uri TEXT NOT NULL,
			cid TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS org_backfill_phases (
			phase TEXT PRIMARY KEY,
			offset INTEGER NOT NULL DEFAULT 0,
			done INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS org_credit_linked (
			involved_company_id TEXT PRIMARY KEY
		);
	`)

	const getPhase = db.prepare('SELECT offset, done FROM org_backfill_phases WHERE phase = ?')
	const upsertPhase = db.prepare(
		`INSERT INTO org_backfill_phases (phase, offset, done) VALUES (?, ?, ?)
		 ON CONFLICT(phase) DO UPDATE SET offset = excluded.offset, done = excluded.done`,
	)
	const setCid = db.prepare(
		'INSERT OR REPLACE INTO org_profile_cids (igdb_id, at_uri, cid) VALUES (?, ?, ?)',
	)
	const getCid = db.prepare(
		'SELECT at_uri, cid FROM org_profile_cids WHERE igdb_id = ?',
	)
	const markLinked = db.prepare(
		'INSERT OR IGNORE INTO org_credit_linked (involved_company_id) VALUES (?)',
	)
	const isLinked = db.prepare(
		'SELECT 1 FROM org_credit_linked WHERE involved_company_id = ?',
	)

	// In-memory cache of org CIDs for Phase 2 hot-path.
	const orgCids = new Map<number, { uri: string; cid: string }>()
	const cidRows = db.prepare('SELECT igdb_id, at_uri, cid FROM org_profile_cids').all() as Array<{
		igdb_id: string; at_uri: string; cid: string
	}>
	for (const row of cidRows) {
		orgCids.set(Number(row.igdb_id), { uri: row.at_uri, cid: row.cid })
	}
	console.log(`[backfill-orgs] Loaded ${orgCids.size} existing org profile CIDs from state`)

	const shutdown = () => {
		console.log('\n[shutdown] Closing...')
		state.close()
		process.exit(0)
	}
	process.on('SIGINT', shutdown)
	process.on('SIGTERM', shutdown)

	// =========================================================================
	// PHASE 1: Create org profiles from IGDB companies
	// =========================================================================
	const phase1 = getPhase.get('createProfiles') as { offset: number; done: number } | undefined
	if (phase1?.done) {
		console.log('[backfill-orgs] Phase 1 (create profiles) already done, skipping.')
	} else {
		console.log('\n=== Phase 1: Create org profiles ===')
		const startOffset = phase1?.offset ?? 0

		const fields = [
			'fields name, slug, description, country, start_date, start_date_category,',
			'logo.image_id, logo.width, logo.height,',
			'websites.url, websites.category, websites.type,',
			'parent, status, updated_at;',
		].join(' ')

		let created = 0
		let skippedExisting = 0
		let lastOffset = startOffset

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
					if (!result) continue
					state.setEntity('org', entry.company.id, result.uri)
					setCid.run(String(entry.company.id), result.uri, result.cid)
					orgCids.set(entry.company.id, { uri: result.uri, cid: result.cid })
					created++
				}
				console.log(`  [batch] Created ${pending.length} profiles (${created} total)`)
			} catch (err) {
				console.warn(`  [!] Batch failed (${(err as Error).message}), falling back to individual creates`)
				for (const entry of pending) {
					try {
						const result = await atproto.createRecord(COLLECTION_PROFILE, entry.record)
						state.setEntity('org', entry.company.id, result.uri)
						setCid.run(String(entry.company.id), result.uri, result.cid)
						orgCids.set(entry.company.id, { uri: result.uri, cid: result.cid })
						created++
					} catch (innerErr) {
						console.error(`  [!] ${errorLabel(innerErr)} ${entry.company.name}: ${(innerErr as Error).message}`)
						state.addFailure('org', entry.company.id)
					}
				}
			}
			pending = []
		}

		for await (const { items, offset: currentOffset } of igdb.paginate<IGDBCompany>(
			'companies',
			fields,
			startOffset,
		)) {
			for (const company of items) {
				if (orgCids.has(company.id) || state.hasEntity('org', company.id)) {
					skippedExisting++
					continue
				}
				const record = mapCompany(company, state, orgCids)
				pending.push({ company, record })
				if (pending.length >= CREATE_BATCH_SIZE) {
					await flushCreates()
				}
			}
			lastOffset = currentOffset + items.length
			upsertPhase.run('createProfiles', lastOffset, 0)
		}

		await flushCreates()
		upsertPhase.run('createProfiles', lastOffset, 1)
		console.log(`[backfill-orgs] Phase 1 done: created ${created}, skipped ${skippedExisting} existing`)
	}

	// =========================================================================
	// PHASE 2: Update org credits with org strongRefs
	// =========================================================================
	const phase2 = getPhase.get('linkCredits') as { offset: number; done: number } | undefined
	if (phase2?.done) {
		console.log('[backfill-orgs] Phase 2 (link credits) already done, skipping.')
		state.close()
		return
	}

	console.log('\n=== Phase 2: Link org credits ===')

	// Load all existing org.credit records from HappyView so we can preserve
	// their existing fields when we update them.
	console.log('[backfill-orgs] Loading org.credit records from HappyView...')
	const sql = postgres(HAPPYVIEW_DATABASE_URL)
	const creditRows = await sql`
		SELECT uri, rkey, record
		FROM records
		WHERE collection = ${COLLECTION_CREDIT}
	` as Array<{ uri: string; rkey: string; record: Record<string, unknown> }>
	const creditsByUri = new Map<string, { rkey: string; record: Record<string, unknown> }>()
	for (const row of creditRows) {
		creditsByUri.set(row.uri, { rkey: row.rkey, record: row.record })
	}
	console.log(`  Loaded ${creditsByUri.size} credit records from HappyView`)

	const startOffset2 = phase2?.offset ?? 0

	const fields2 = [
		'fields company, game, developer, publisher, porting, supporting, updated_at;',
	].join(' ')

	let linked = 0
	let alreadyLinked = 0
	let missingCredit = 0
	let missingOrg = 0
	let noCompany = 0
	let lastOffset2 = startOffset2

	let updateBatch: Array<{ rkey: string; record: Record<string, unknown>; involvedId: number }> = []

	const flushUpdates = async () => {
		if (updateBatch.length === 0) return
		try {
			await atproto.applyUpdates(
				updateBatch.map((u) => ({
					collection: COLLECTION_CREDIT,
					rkey: u.rkey,
					record: u.record,
				})),
			)
			for (const entry of updateBatch) {
				markLinked.run(String(entry.involvedId))
				linked++
			}
			console.log(`  [batch] Linked ${updateBatch.length} credits (${linked} total)`)
		} catch (err) {
			console.warn(`  [!] Batch failed (${(err as Error).message}), falling back to putRecord`)
			for (const entry of updateBatch) {
				try {
					await atproto.putRecord(COLLECTION_CREDIT, entry.rkey, entry.record)
					markLinked.run(String(entry.involvedId))
					linked++
				} catch (innerErr) {
					console.error(`  [!] ${errorLabel(innerErr)} credit ${entry.involvedId}: ${(innerErr as Error).message}`)
				}
			}
		}
		updateBatch = []
	}

	for await (const { items, offset: currentOffset } of igdb.paginate<IGDBInvolvedCompany>(
		'involved_companies',
		fields2,
		startOffset2,
	)) {
		for (const ic of items) {
			if (isLinked.get(String(ic.id))) {
				alreadyLinked++
				continue
			}

			// Look up the credit record
			const creditUri = state.getEntity('orgCredit', ic.id)
			if (!creditUri) { missingCredit++; continue }

			const existing = creditsByUri.get(creditUri)
			if (!existing) { missingCredit++; continue }

			// Look up the company
			const companyId = typeof ic.company === 'object' ? ic.company?.id : ic.company
			if (!companyId) { noCompany++; continue }

			let orgEntry = orgCids.get(companyId)
			if (!orgEntry) {
				const row = getCid.get(String(companyId)) as { at_uri: string; cid: string } | undefined
				if (row) {
					orgEntry = { uri: row.at_uri, cid: row.cid }
					orgCids.set(companyId, orgEntry)
				}
			}
			if (!orgEntry) { missingOrg++; continue }

			// Merge the org strongRef into the existing record
			const record = { ...existing.record }
			delete record.$type
			record.org = { uri: orgEntry.uri, cid: orgEntry.cid }

			updateBatch.push({ rkey: existing.rkey, record, involvedId: ic.id })
			if (updateBatch.length >= UPDATE_BATCH_SIZE) {
				await flushUpdates()
			}
		}
		lastOffset2 = currentOffset + items.length
		upsertPhase.run('linkCredits', lastOffset2, 0)
	}

	await flushUpdates()
	upsertPhase.run('linkCredits', lastOffset2, 1)

	console.log()
	console.log('=== Backfill complete ===')
	console.log(`  Linked: ${linked}`)
	console.log(`  Already linked (resumed): ${alreadyLinked}`)
	console.log(`  Missing credit record: ${missingCredit}`)
	console.log(`  Missing org profile: ${missingOrg}`)
	console.log(`  No company on IGDB record: ${noCompany}`)

	state.close()
	await sql.end()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
