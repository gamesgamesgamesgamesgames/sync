/**
 * Re-download all IGDB images at 1080p and replace existing PDS blobs.
 *
 * Phase 1 — Re-upload blobs:
 *   For each image_id in the blob cache, download at t_1080p from IGDB,
 *   upload to PDS, and update the blob cache with the new CID.
 *
 * Phase 2 — Update records:
 *   For each game/platform/engine/profile record, fetch it from the PDS,
 *   replace any old blob CIDs with new ones, and putRecord back.
 *
 * Phase 3 — Delete orphaned blobs:
 *   Delete old blobs from R2 that are no longer referenced by any record.
 *
 * Resumable: tracks progress in a blob_backfill_progress table.
 *
 * Usage: npx tsx src/backfill-blobs-1080p.ts [--phase2-only]
 */

import 'dotenv/config'
import { AtpAgent } from '@atproto/api'
import { S3Client, DeleteObjectsCommand } from '@aws-sdk/client-s3'
import Database from 'better-sqlite3'
import { countdownSleep } from './helpers.js'

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const PDS_SERVICE = process.env.ATPROTO_SERVICE!
const PDS_IDENTIFIER = process.env.ATPROTO_IDENTIFIER!
const PDS_PASSWORD = process.env.ATPROTO_PASSWORD!
const STATE_DB_PATH = process.env.STATE_DB_PATH ?? 'state.sqlite'

const BLOB_BATCH_SIZE = 50
const DOWNLOAD_CONCURRENCY = 20
const UPLOAD_CONCURRENCY = 10
const RECORD_BATCH_SIZE = 200

// ---------------------------------------------------------------------------
// Concurrency pool
// ---------------------------------------------------------------------------

class Pool {
	private running = 0
	private queue: Array<() => void> = []

	constructor(private max: number) {}

	async run<T>(fn: () => Promise<T>): Promise<T> {
		if (this.running >= this.max) {
			await new Promise<void>((resolve) => this.queue.push(resolve))
		}
		this.running++
		try {
			return await fn()
		} finally {
			this.running--
			const next = this.queue.shift()
			if (next) next()
		}
	}
}

// ---------------------------------------------------------------------------
// IGDB image download
// ---------------------------------------------------------------------------

async function downloadImage(imageId: string): Promise<Buffer> {
	const url = `https://images.igdb.com/igdb/image/upload/t_1080p/${imageId}.jpg`
	const response = await fetch(url)
	if (!response.ok) {
		throw new Error(`${response.status}`)
	}
	return Buffer.from(await response.arrayBuffer())
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

let shuttingDown = false
function onShutdown() {
	if (shuttingDown) return
	shuttingDown = true
	console.log('\n[backfill-blobs] Shutting down gracefully after current batch...')
}

// ---------------------------------------------------------------------------
// Phase 1: Re-upload blobs
// ---------------------------------------------------------------------------

async function phase1(agent: AtpAgent, db: Database.Database): Promise<Map<string, string>> {
	console.log('\n=== Phase 1: Re-uploading blobs at 1080p ===')

	db.exec(`
		CREATE TABLE IF NOT EXISTS blob_backfill_progress (
			image_id TEXT PRIMARY KEY,
			old_cid TEXT,
			new_cid TEXT
		)
	`)

	const totalRow = db.prepare('SELECT COUNT(*) as count FROM blobs').get() as { count: number }
	const doneRow = db.prepare('SELECT COUNT(*) as count FROM blob_backfill_progress').get() as { count: number }
	console.log(`Total blobs: ${totalRow.count}, already processed: ${doneRow.count}, remaining: ${totalRow.count - doneRow.count}`)

	const getUnprocessed = db.prepare(`
		SELECT b.image_id, b.ref_link, b.mime_type, b.size
		FROM blobs b
		LEFT JOIN blob_backfill_progress p ON b.image_id = p.image_id
		WHERE p.image_id IS NULL
		LIMIT ?
	`)

	const markDone = db.prepare('INSERT OR IGNORE INTO blob_backfill_progress (image_id, old_cid, new_cid) VALUES (?, ?, ?)')
	const updateBlob = db.prepare('INSERT OR REPLACE INTO blobs (image_id, ref_link, mime_type, size) VALUES (?, ?, ?, ?)')

	const dlPool = new Pool(DOWNLOAD_CONCURRENCY)
	const ulPool = new Pool(UPLOAD_CONCURRENCY)

	let processed = doneRow.count
	let updated = 0
	let skipped = 0
	let failed = 0

	while (!shuttingDown) {
		const rows = getUnprocessed.all(BLOB_BATCH_SIZE) as Array<{
			image_id: string
			ref_link: string
			mime_type: string
			size: number
		}>
		if (rows.length === 0) break

		const results = await Promise.allSettled(
			rows.map(async (row) => {
				let imageBuffer: Buffer
				try {
					imageBuffer = await dlPool.run(() => downloadImage(row.image_id))
				} catch (err) {
					console.warn(`  [!] Download failed ${row.image_id}: ${(err as Error).message}`)
					throw err
				}

				let newCid: string
				let newSize: number
				try {
					const resp = await ulPool.run(async () => {
						const r = await agent.uploadBlob(imageBuffer, { encoding: 'image/jpeg' })
						return { cid: r.data.blob.ref.toString(), size: r.data.blob.size }
					})
					newCid = resp.cid
					newSize = resp.size
				} catch (err) {
					console.warn(`  [!] Upload failed ${row.image_id}: ${(err as Error).message}`)
					throw err
				}

				return { image_id: row.image_id, oldCid: row.ref_link, newCid, newSize }
			}),
		)

		const transaction = db.transaction(() => {
			for (const result of results) {
				if (result.status === 'rejected') {
					failed++
					continue
				}
				const { image_id, oldCid, newCid, newSize } = result.value
				if (newCid === oldCid) {
					skipped++
					markDone.run(image_id, oldCid, newCid)
				} else {
					updated++
					updateBlob.run(image_id, newCid, 'image/jpeg', newSize)
					markDone.run(image_id, oldCid, newCid)
				}
			}
		})
		transaction()

		processed += rows.length
		if (processed % 500 === 0 || rows.length < BLOB_BATCH_SIZE) {
			console.log(`[phase1] ${processed}/${totalRow.count} (updated=${updated}, skipped=${skipped}, failed=${failed})`)
		}
	}

	console.log(`\n[phase1] Complete. updated=${updated}, skipped=${skipped}, failed=${failed}`)

	// Build the CID mapping for phase 2
	const cidMap = new Map<string, string>()
	const mappingRows = db.prepare('SELECT old_cid, new_cid FROM blob_backfill_progress WHERE old_cid != new_cid').all() as Array<{
		old_cid: string
		new_cid: string
	}>
	for (const row of mappingRows) {
		cidMap.set(row.old_cid, row.new_cid)
	}
	console.log(`[phase1] ${cidMap.size} CIDs changed`)
	return cidMap
}

// ---------------------------------------------------------------------------
// Phase 2: Update records with new blob CIDs
// ---------------------------------------------------------------------------

/**
 * Recursively walk a record and replace any blob ref $link values
 * that appear in the cidMap. Returns true if any replacement was made.
 */
function replaceBlobRefs(obj: unknown, cidMap: Map<string, string>): boolean {
	if (obj === null || typeof obj !== 'object') return false

	if (Array.isArray(obj)) {
		let changed = false
		for (const item of obj) {
			if (replaceBlobRefs(item, cidMap)) changed = true
		}
		return changed
	}

	const record = obj as Record<string, unknown>
	let changed = false

	// Check if this is a blob ref: { $type: "blob", ref: { $link: "..." }, ... }
	if (record.$type === 'blob' && record.ref && typeof record.ref === 'object') {
		const ref = record.ref as Record<string, unknown>
		if (typeof ref.$link === 'string') {
			const newCid = cidMap.get(ref.$link)
			if (newCid) {
				ref.$link = newCid
				changed = true
			}
		}
	}

	// Recurse into all values
	for (const value of Object.values(record)) {
		if (replaceBlobRefs(value, cidMap)) changed = true
	}

	return changed
}

async function phase2(agent: AtpAgent, db: Database.Database, cidMap: Map<string, string>): Promise<void> {
	console.log('\n=== Phase 2: Updating records with new blob CIDs ===')

	if (cidMap.size === 0) {
		console.log('[phase2] No CID changes to apply. Done.')
		return
	}

	const did = agent.session!.did

	// Track phase 2 progress
	db.exec(`
		CREATE TABLE IF NOT EXISTS blob_backfill_records_progress (
			uri TEXT PRIMARY KEY
		)
	`)

	const markRecordDone = db.prepare('INSERT OR IGNORE INTO blob_backfill_records_progress (uri) VALUES (?)')
	const isRecordDone = db.prepare('SELECT 1 FROM blob_backfill_records_progress WHERE uri = ?')

	// Collections that can contain blobs
	const collections = [
		'games.gamesgamesgamesgames.game',
		'games.gamesgamesgamesgames.platform',
		'games.gamesgamesgamesgames.engine',
		'games.gamesgamesgamesgames.actor.profile',
		'games.gamesgamesgamesgames.org.profile',
	]

	let totalUpdated = 0
	let totalSkipped = 0
	let totalFailed = 0

	for (const collection of collections) {
		if (shuttingDown) break

		console.log(`\n[phase2] Processing ${collection}...`)
		let cursor: string | undefined
		let collectionUpdated = 0

		while (!shuttingDown) {
			const params: Record<string, unknown> = {
				repo: did,
				collection,
				limit: RECORD_BATCH_SIZE,
			}
			if (cursor) params.cursor = cursor

			let response
			try {
				response = await agent.com.atproto.repo.listRecords(params as any)
			} catch (err) {
				console.error(`  [!] listRecords failed: ${(err as Error).message}`)
				break
			}

			const records = response.data.records
			if (records.length === 0) break

			const updates: Array<{ collection: string; rkey: string; record: Record<string, unknown> }> = []

			for (const entry of records) {
				if (isRecordDone.get(entry.uri)) {
					totalSkipped++
					continue
				}

				const record = entry.value as Record<string, unknown>
				const changed = replaceBlobRefs(record, cidMap)

				if (changed) {
					const rkey = entry.uri.split('/').pop()!
					updates.push({ collection, rkey, record })
				} else {
					markRecordDone.run(entry.uri)
					totalSkipped++
				}
			}

			// Apply updates in batches via applyWrites
			if (updates.length > 0) {
				try {
					await agent.com.atproto.repo.applyWrites({
						repo: did,
						writes: updates.map((u) => ({
							$type: 'com.atproto.repo.applyWrites#update',
							collection: u.collection,
							rkey: u.rkey,
							value: { $type: u.collection, ...u.record },
						})),
					})

					const transaction = db.transaction(() => {
						for (const u of updates) {
							const uri = `at://${did}/${u.collection}/${u.rkey}`
							markRecordDone.run(uri)
						}
					})
					transaction()

					collectionUpdated += updates.length
					totalUpdated += updates.length
				} catch (err) {
					console.error(`  [!] applyWrites failed for batch of ${updates.length}: ${(err as Error).message}`)
					// Fall back to individual puts
					for (const u of updates) {
						try {
							await agent.com.atproto.repo.putRecord({
								repo: did,
								collection: u.collection,
								rkey: u.rkey,
								record: { $type: u.collection, ...u.record },
							})
							const uri = `at://${did}/${u.collection}/${u.rkey}`
							markRecordDone.run(uri)
							collectionUpdated++
							totalUpdated++
						} catch (putErr) {
							console.error(`  [!] putRecord failed for ${u.rkey}: ${(putErr as Error).message}`)
							totalFailed++
						}
					}
				}
			}

			cursor = response.data.cursor
			if (!cursor) break

			if ((totalUpdated + totalSkipped) % 2000 === 0) {
				console.log(`  [phase2] ${collection}: updated=${collectionUpdated} (total records scanned: ${totalUpdated + totalSkipped + totalFailed})`)
			}
		}

		console.log(`  [phase2] ${collection}: ${collectionUpdated} records updated`)
	}

	console.log(`\n[phase2] Complete. updated=${totalUpdated}, skipped=${totalSkipped}, failed=${totalFailed}`)
}

// ---------------------------------------------------------------------------
// Phase 3: Delete orphaned blobs from R2
// ---------------------------------------------------------------------------

async function phase3(db: Database.Database, cidMap: Map<string, string>, did: string): Promise<void> {
	console.log('\n=== Phase 3: Deleting orphaned blobs from R2 ===')

	if (cidMap.size === 0) {
		console.log('[phase3] No orphaned blobs to delete. Done.')
		return
	}

	const R2_ENDPOINT = process.env.R2_ENDPOINT ?? process.env.PDS_BLOBSTORE_S3_ENDPOINT
	const R2_ACCESS_KEY = process.env.R2_ACCESS_KEY_ID ?? process.env.PDS_BLOBSTORE_S3_ACCESS_KEY_ID
	const R2_SECRET_KEY = process.env.R2_SECRET_ACCESS_KEY ?? process.env.PDS_BLOBSTORE_S3_SECRET_ACCESS_KEY
	const R2_BUCKET = process.env.R2_BUCKET ?? process.env.PDS_BLOBSTORE_S3_BUCKET

	if (!R2_ENDPOINT || !R2_ACCESS_KEY || !R2_SECRET_KEY || !R2_BUCKET) {
		console.error('[phase3] Missing R2/S3 credentials. Set R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET (or PDS_BLOBSTORE_S3_* equivalents).')
		console.log('[phase3] Skipping blob deletion.')
		return
	}

	const s3 = new S3Client({
		region: 'auto',
		endpoint: R2_ENDPOINT,
		credentials: {
			accessKeyId: R2_ACCESS_KEY,
			secretAccessKey: R2_SECRET_KEY,
		},
		forcePathStyle: true,
	})

	// Collect all old CIDs that were replaced
	const oldCids = [...cidMap.keys()]
	console.log(`[phase3] ${oldCids.length} orphaned blobs to delete`)

	// R2/S3 deleteObjects supports up to 1000 keys per request
	const DELETE_BATCH = 1000
	let deleted = 0
	let failed = 0

	for (let i = 0; i < oldCids.length && !shuttingDown; i += DELETE_BATCH) {
		const batch = oldCids.slice(i, i + DELETE_BATCH)
		const keys = batch.map((cid) => ({ Key: `blocks/${did}/${cid}` }))

		try {
			const result = await s3.send(new DeleteObjectsCommand({
				Bucket: R2_BUCKET,
				Delete: { Objects: keys },
			}))
			deleted += result.Deleted?.length ?? batch.length
			if (result.Errors && result.Errors.length > 0) {
				failed += result.Errors.length
				for (const err of result.Errors.slice(0, 3)) {
					console.warn(`  [!] Delete failed for ${err.Key}: ${err.Message}`)
				}
			}
		} catch (err) {
			console.error(`  [!] Batch delete failed: ${(err as Error).message}`)
			failed += batch.length
		}

		if ((i + DELETE_BATCH) % 10000 === 0 || i + DELETE_BATCH >= oldCids.length) {
			console.log(`[phase3] Progress: ${Math.min(i + DELETE_BATCH, oldCids.length)}/${oldCids.length} (deleted=${deleted}, failed=${failed})`)
		}
	}

	console.log(`\n[phase3] Complete. deleted=${deleted}, failed=${failed}`)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
	if (!PDS_SERVICE || !PDS_IDENTIFIER || !PDS_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD')
	}

	process.on('SIGINT', onShutdown)
	process.on('SIGTERM', onShutdown)

	const agent = new AtpAgent({ service: PDS_SERVICE })
	await agent.login({ identifier: PDS_IDENTIFIER, password: PDS_PASSWORD })
	console.log(`[backfill-blobs] Logged in as ${agent.session!.did}`)

	const db = new Database(STATE_DB_PATH)
	db.pragma('journal_mode = WAL')

	const phase2Only = process.argv.includes('--phase2-only')

	let cidMap: Map<string, string>

	if (phase2Only) {
		console.log('[backfill-blobs] Skipping phase 1 (--phase2-only)')
		const rows = db.prepare('SELECT old_cid, new_cid FROM blob_backfill_progress WHERE old_cid != new_cid').all() as Array<{
			old_cid: string
			new_cid: string
		}>
		cidMap = new Map(rows.map((r) => [r.old_cid, r.new_cid]))
		console.log(`[backfill-blobs] Loaded ${cidMap.size} CID mappings from previous run`)
	} else {
		cidMap = await phase1(agent, db)
	}

	if (!shuttingDown) {
		await phase2(agent, db, cidMap)
	}

	if (!shuttingDown) {
		await phase3(db, cidMap, agent.session!.did)
	}

	if (!shuttingDown) {
		// Clean up progress tables
		console.log('\n[backfill-blobs] Cleaning up progress tables...')
		db.exec('DROP TABLE IF EXISTS blob_backfill_progress')
		db.exec('DROP TABLE IF EXISTS blob_backfill_records_progress')
	}

	db.close()
	console.log('\n[backfill-blobs] Done.')
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
