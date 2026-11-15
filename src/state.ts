/**
 * State management for resume support.
 *
 * Persists progress to state.sqlite so the scraper can pick up
 * where it left off if interrupted.
 *
 * Uses SQLite for instant startup, indexed lookups, incremental
 * writes, and dramatically lower memory usage vs JSON.
 */

import Database from 'better-sqlite3'

/**
 * How long a contributor blob confirmation is trusted when no contribution
 * references the blob. The contributor's PDS (Tranquil) deletes unreferenced
 * blobs after a 6-hour grace period, checked hourly.
 */
export const CONTRIBUTOR_BLOB_TRUST_SECONDS = 5 * 3600

const STATE_FILE = 'state.sqlite'

export type PhaseName =
	| 'platformFamilies'
	| 'platforms'
	| 'engines'
	| 'collections'
	| 'games'
	| 'credits'
	| 'syncGames'

export type EntityType =
	| 'platformFamily'
	| 'platform'
	| 'engine'
	| 'collection'
	| 'game'
	| 'org'
	| 'orgCredit'

export interface BlobCacheEntry {
	ref: { $link: string }
	mimeType: string
	size: number
}

export interface SteamMapping {
	igdbId: string
	steamId: string
	atUri: string
	enrichedAt: number | null
}

const ALL_PHASES: PhaseName[] = [
	'platformFamilies',
	'platforms',
	'engines',
	'collections',
	'games',
	'credits',
	'syncGames',
]

export class StateManager {
	private db: Database.Database

	/** Expose the underlying SQLite database for ad-hoc tables (e.g. backfill progress). */
	getDb(): Database.Database {
		return this.db
	}

	constructor(dbPath?: string) {
		this.db = new Database(dbPath ?? STATE_FILE)
		this.db.pragma('journal_mode = WAL')
		this.db.pragma('synchronous = NORMAL')
		this.createTables()
	}

	private createTables(): void {
		this.db.exec(`
			CREATE TABLE IF NOT EXISTS meta (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS phases (
				name TEXT PRIMARY KEY,
				offset INTEGER NOT NULL DEFAULT 0,
				done INTEGER NOT NULL DEFAULT 0
			);

			CREATE TABLE IF NOT EXISTS entities (
				type TEXT NOT NULL,
				igdb_id TEXT NOT NULL,
				at_uri TEXT NOT NULL,
				PRIMARY KEY (type, igdb_id)
			);

			CREATE TABLE IF NOT EXISTS blobs (
				image_id TEXT PRIMARY KEY,
				ref_link TEXT NOT NULL,
				mime_type TEXT NOT NULL,
				size INTEGER NOT NULL
			);

			CREATE TABLE IF NOT EXISTS failures (
				type TEXT NOT NULL,
				igdb_id TEXT NOT NULL,
				PRIMARY KEY (type, igdb_id)
			);

			-- Consecutive runs in which an entity failed in a way a rerun might
			-- fix. After enough of them the entity moves to failures and stops
			-- holding the sync cursor back.
			CREATE TABLE IF NOT EXISTS failure_streaks (
				type TEXT NOT NULL,
				igdb_id TEXT NOT NULL,
				runs INTEGER NOT NULL,
				last_run_at INTEGER NOT NULL,
				last_error TEXT,
				PRIMARY KEY (type, igdb_id)
			);

			-- Cached blobs known to be in the catalogue repo as well as the
			-- contributor's.
			CREATE TABLE IF NOT EXISTS blob_mirrors (
				image_id TEXT PRIMARY KEY,
				cid TEXT
			);

			-- Cached blobs known to be in the contributor's (the sync account's)
			-- repo. Blobs cached before 2026-06-18 were uploaded to the catalogue
			-- repo only, and a contribution can't reference those.
			CREATE TABLE IF NOT EXISTS blob_contributor (
				image_id TEXT PRIMARY KEY,
				cid TEXT
			);

			CREATE TABLE IF NOT EXISTS steam_map (
				igdb_id TEXT PRIMARY KEY,
				steam_id TEXT NOT NULL,
				at_uri TEXT NOT NULL,
				enriched_at INTEGER
			);

			-- Games whose org.credit records still need writing. involved is the
			-- game's IGDB involved_companies as JSON, or NULL until fetched.
			CREATE TABLE IF NOT EXISTS credit_queue (
				igdb_id INTEGER PRIMARY KEY,
				involved TEXT,
				enqueued_at INTEGER NOT NULL,
				attempts INTEGER NOT NULL DEFAULT 0,
				last_error TEXT
			);

			-- New games not proposed because their IGDB game_type doesn't map to
			-- an applicationType. Re-checked each run; released once proposed.
			CREATE TABLE IF NOT EXISTS held_games (
				igdb_id INTEGER PRIMARY KEY,
				game_type TEXT,
				held_at INTEGER NOT NULL
			);

			CREATE TABLE IF NOT EXISTS transformer_progress (
				name TEXT NOT NULL,
				entity_type TEXT NOT NULL,
				offset INTEGER NOT NULL DEFAULT 0,
				done INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (name, entity_type)
			);
		`)

		// Insert default meta if missing
		const existing = this.db.prepare('SELECT 1 FROM meta WHERE key = ?').get('phase')
		if (!existing) {
			this.db.prepare('INSERT INTO meta (key, value) VALUES (?, ?)').run('phase', 'platformFamilies')
		}

		// Insert default phase rows if missing
		const insertPhase = this.db.prepare(
			'INSERT OR IGNORE INTO phases (name, offset, done) VALUES (?, 0, 0)',
		)
		for (const phase of ALL_PHASES) {
			insertPhase.run(phase)
		}

		// The blob ledgers record the CID each copy was made at; an entry only
		// counts when (image_id, cid) both match. Ledgers created before the
		// column existed get it added empty, which counts as unverified.
		for (const table of ['blob_mirrors', 'blob_contributor']) {
			const columns = this.db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>
			if (!columns.some((c) => c.name === 'cid')) {
				this.db.exec(`ALTER TABLE ${table} ADD COLUMN cid TEXT`)
			}
		}

		// The contributor's PDS deletes blobs no record references once they are
		// a few hours old, so a contributor entry records when it was confirmed
		// and whether a created contribution references it. Older ledgers get
		// both columns empty: unconfirmed, so their blobs are checked again.
		const contributorColumns = this.db.prepare('PRAGMA table_info(blob_contributor)').all() as Array<{ name: string }>
		if (!contributorColumns.some((c) => c.name === 'confirmed_at')) {
			this.db.exec('ALTER TABLE blob_contributor ADD COLUMN confirmed_at INTEGER')
		}
		if (!contributorColumns.some((c) => c.name === 'referenced')) {
			this.db.exec('ALTER TABLE blob_contributor ADD COLUMN referenced INTEGER NOT NULL DEFAULT 0')
		}

		console.log(`[state] Opened ${STATE_FILE} (phase: "${this.getCurrentPhase()}")`)
	}

	/** Get the current phase name. */
	getCurrentPhase(): PhaseName {
		const row = this.db.prepare('SELECT value FROM meta WHERE key = ?').get('phase') as
			| { value: string }
			| undefined
		return (row?.value ?? 'platformFamilies') as PhaseName
	}

	/** Set the current phase. */
	setPhase(phase: PhaseName): void {
		this.db.prepare('UPDATE meta SET value = ? WHERE key = ?').run(phase, 'phase')
	}

	/** Check if a phase is done. */
	isPhaseDone(phase: PhaseName): boolean {
		const row = this.db.prepare('SELECT done FROM phases WHERE name = ?').get(phase) as
			| { done: number }
			| undefined
		return row?.done === 1
	}

	/** Mark a phase as done. */
	markPhaseDone(phase: PhaseName): void {
		this.db.prepare('UPDATE phases SET done = 1 WHERE name = ?').run(phase)
	}

	/** Get the offset for a phase. */
	getOffset(phase: PhaseName): number {
		const row = this.db.prepare('SELECT offset FROM phases WHERE name = ?').get(phase) as
			| { offset: number }
			| undefined
		return row?.offset ?? 0
	}

	/** Update the offset for a phase. */
	setOffset(phase: PhaseName, offset: number): void {
		this.db.prepare('UPDATE phases SET offset = ? WHERE name = ?').run(offset, phase)
	}

	/** Check if an IGDB ID has already been processed for a given entity type. */
	hasEntity(type: EntityType, igdbId: number | string): boolean {
		const row = this.db
			.prepare('SELECT 1 FROM entities WHERE type = ? AND igdb_id = ?')
			.get(type, String(igdbId))
		return row !== undefined
	}

	/** Get the atproto AT-URI for a given IGDB ID. */
	getEntity(type: EntityType, igdbId: number | string): string | undefined {
		const row = this.db
			.prepare('SELECT at_uri FROM entities WHERE type = ? AND igdb_id = ?')
			.get(type, String(igdbId)) as { at_uri: string } | undefined
		return row?.at_uri
	}

	/** Store the mapping from an IGDB ID to an atproto AT-URI. */
	setEntity(type: EntityType, igdbId: number | string, atUri: string): void {
		this.db
			.prepare('INSERT OR REPLACE INTO entities (type, igdb_id, at_uri) VALUES (?, ?, ?)')
			.run(type, String(igdbId), atUri)
	}

	/** Get the count of entities for a given type. */
	getEntityCount(type: EntityType): number {
		const row = this.db
			.prepare('SELECT COUNT(*) as count FROM entities WHERE type = ?')
			.get(type) as { count: number }
		return row.count
	}

	/** Check if a blob has already been uploaded. */
	hasBlob(imageId: string): boolean {
		const row = this.db.prepare('SELECT 1 FROM blobs WHERE image_id = ?').get(imageId)
		return row !== undefined
	}

	/** Get a cached blob entry. */
	getBlob(imageId: string): BlobCacheEntry | undefined {
		const row = this.db
			.prepare('SELECT ref_link, mime_type, size FROM blobs WHERE image_id = ?')
			.get(imageId) as { ref_link: string; mime_type: string; size: number } | undefined
		if (!row) return undefined
		return {
			ref: { $link: row.ref_link },
			mimeType: row.mime_type,
			size: row.size,
		}
	}

	/** Cache a blob upload result. */
	setBlob(imageId: string, entry: BlobCacheEntry): void {
		this.db
			.prepare('INSERT OR REPLACE INTO blobs (image_id, ref_link, mime_type, size) VALUES (?, ?, ?, ?)')
			.run(imageId, entry.ref.$link, entry.mimeType, entry.size)
	}

	/** Record a failure for manual review. */
	addFailure(type: EntityType, igdbId: number | string): void {
		this.db
			.prepare('INSERT OR IGNORE INTO failures (type, igdb_id) VALUES (?, ?)')
			.run(type, String(igdbId))
	}

	/** Get all failure IDs for a given entity type. */
	getFailures(type: EntityType): string[] {
		const rows = this.db
			.prepare('SELECT igdb_id FROM failures WHERE type = ?')
			.all(type) as Array<{ igdb_id: string }>
		return rows.map((r) => r.igdb_id)
	}

	/** Get failure count, optionally filtered by type. */
	getFailureCount(type?: EntityType): number {
		if (type) {
			const row = this.db
				.prepare('SELECT COUNT(*) as count FROM failures WHERE type = ?')
				.get(type) as { count: number }
			return row.count
		}
		const row = this.db.prepare('SELECT COUNT(*) as count FROM failures').get() as { count: number }
		return row.count
	}

	/**
	 * Count a failed run for an entity and return how many consecutive runs it
	 * has now failed in. Several failures within one run (`runAt`) count once.
	 */
	recordFailedRun(type: EntityType, igdbId: number | string, runAt: number, error: string): number {
		const id = String(igdbId)
		const row = this.db
			.prepare('SELECT runs, last_run_at FROM failure_streaks WHERE type = ? AND igdb_id = ?')
			.get(type, id) as { runs: number; last_run_at: number } | undefined
		const runs = !row ? 1 : row.last_run_at === runAt ? row.runs : row.runs + 1
		this.db
			.prepare('INSERT OR REPLACE INTO failure_streaks (type, igdb_id, runs, last_run_at, last_error) VALUES (?, ?, ?, ?, ?)')
			.run(type, id, runs, runAt, error.slice(0, 500))
		return runs
	}

	/** Forget an entity's failure streak after it succeeds. */
	clearFailureStreak(type: EntityType, igdbId: number | string): void {
		this.db
			.prepare('DELETE FROM failure_streaks WHERE type = ? AND igdb_id = ?')
			.run(type, String(igdbId))
	}

	/** How many consecutive runs an entity has failed in (0 if none). */
	getFailureStreak(type: EntityType, igdbId: number | string): number {
		const row = this.db
			.prepare('SELECT runs FROM failure_streaks WHERE type = ? AND igdb_id = ?')
			.get(type, String(igdbId)) as { runs: number } | undefined
		return row?.runs ?? 0
	}

	/** Whether the catalogue repo is known to hold this image's blob at this CID. */
	isBlobMirrored(imageId: string, cid: string): boolean {
		return !!this.db.prepare('SELECT 1 FROM blob_mirrors WHERE image_id = ? AND cid = ?').get(imageId, cid)
	}

	/** Record that the catalogue repo holds this image's blob at this CID. */
	setBlobMirrored(imageId: string, cid: string): void {
		this.db.prepare('INSERT OR REPLACE INTO blob_mirrors (image_id, cid) VALUES (?, ?)').run(imageId, cid)
	}

	/** Forget the catalogue copy of an image's blob. */
	clearBlobMirrored(imageId: string): void {
		this.db.prepare('DELETE FROM blob_mirrors WHERE image_id = ?').run(imageId)
	}

	/** Whether the contributor's repo is known to hold this image's blob at this CID. */
	/**
	 * Whether the contributor's repo can be trusted to hold this image's blob
	 * at this CID: a created contribution references it, or it was confirmed
	 * less than CONTRIBUTOR_BLOB_TRUST_SECONDS ago. Anything else may have been
	 * deleted as unreferenced and is checked again.
	 */
	isBlobInContributor(imageId: string, cid: string): boolean {
		const since = Math.floor(Date.now() / 1000) - CONTRIBUTOR_BLOB_TRUST_SECONDS
		return !!this.db.prepare(
			'SELECT 1 FROM blob_contributor WHERE image_id = ? AND cid = ? AND (referenced = 1 OR confirmed_at >= ?)',
		).get(imageId, cid, since)
	}

	/** Record that the contributor's repo holds this image's blob at this CID, as of now. */
	setBlobInContributor(imageId: string, cid: string): void {
		this.db.prepare(
			`INSERT INTO blob_contributor (image_id, cid, confirmed_at, referenced) VALUES (?, ?, ?, 0)
			 ON CONFLICT (image_id) DO UPDATE SET
			   cid = excluded.cid,
			   confirmed_at = excluded.confirmed_at,
			   referenced = CASE WHEN blob_contributor.cid = excluded.cid THEN blob_contributor.referenced ELSE 0 END`,
		).run(imageId, cid, Math.floor(Date.now() / 1000))
	}

	/** Record that a created contribution references these blobs, so they are kept. */
	markContributorBlobsReferenced(refs: Array<{ imageId: string; cid: string }>): void {
		const stmt = this.db.prepare('UPDATE blob_contributor SET referenced = 1 WHERE image_id = ? AND cid = ?')
		for (const r of refs) stmt.run(r.imageId, r.cid)
	}

	/** Forget the contributor copy of an image's blob. */
	clearBlobInContributor(imageId: string): void {
		this.db.prepare('DELETE FROM blob_contributor WHERE image_id = ?').run(imageId)
	}

	/** Remove a failure entry after a successful retry. */
	removeFailure(type: EntityType, id: string | number): void {
		this.db
			.prepare('DELETE FROM failures WHERE type = ? AND igdb_id = ?')
			.run(type, String(id))
	}

	/** Insert or update a steam mapping. On conflict, preserves existing enriched_at. */
	setSteamMapping(igdbId: string, steamId: string, atUri: string): void {
		this.db
			.prepare(
				`INSERT INTO steam_map (igdb_id, steam_id, at_uri)
				 VALUES (?, ?, ?)
				 ON CONFLICT(igdb_id) DO UPDATE SET steam_id = excluded.steam_id, at_uri = excluded.at_uri`,
			)
			.run(igdbId, steamId, atUri)
	}

	/** Get a steam mapping by IGDB ID. */
	getSteamMapping(igdbId: string): SteamMapping | undefined {
		const row = this.db
			.prepare('SELECT igdb_id, steam_id, at_uri, enriched_at FROM steam_map WHERE igdb_id = ?')
			.get(igdbId) as { igdb_id: string; steam_id: string; at_uri: string; enriched_at: number | null } | undefined
		if (!row) return undefined
		return {
			igdbId: row.igdb_id,
			steamId: row.steam_id,
			atUri: row.at_uri,
			enrichedAt: row.enriched_at,
		}
	}

	/** Get unenriched steam mappings, up to a limit. */
	getUnenrichedSteamMappings(limit: number): SteamMapping[] {
		const rows = this.db
			.prepare('SELECT igdb_id, steam_id, at_uri, enriched_at FROM steam_map WHERE enriched_at IS NULL LIMIT ?')
			.all(limit) as Array<{ igdb_id: string; steam_id: string; at_uri: string; enriched_at: number | null }>
		return rows.map((row) => ({
			igdbId: row.igdb_id,
			steamId: row.steam_id,
			atUri: row.at_uri,
			enrichedAt: row.enriched_at,
		}))
	}

	/** Mark a steam mapping as enriched with the current timestamp. */
	markSteamEnriched(igdbId: string): void {
		this.db
			.prepare('UPDATE steam_map SET enriched_at = ? WHERE igdb_id = ?')
			.run(Date.now(), igdbId)
	}

	/** No-op — writes are immediate in SQLite. Kept for API compatibility. */
	save(): void {}

	/** No-op — writes are immediate in SQLite. Kept for API compatibility. */
	saveSync(): void {}

	/** No-op — writes are immediate in SQLite. Kept for API compatibility. */
	async flush(): Promise<void> {}

	/** Get the last sync timestamp for an entity type. */
	getLastSyncAt(type: EntityType): number | null {
		const row = this.db
			.prepare('SELECT value FROM meta WHERE key = ?')
			.get(`last_sync_${type}`) as { value: string } | undefined
		return row ? Number(row.value) : null
	}

	/** Set the last sync timestamp for an entity type. */
	setLastSyncAt(type: EntityType, timestamp: number): void {
		this.db
			.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)')
			.run(`last_sync_${type}`, String(timestamp))
	}

	/** Get a numeric value from meta. */
	getMetaNumber(key: string): number | null {
		const row = this.db
			.prepare('SELECT value FROM meta WHERE key = ?')
			.get(key) as { value: string } | undefined
		return row ? Number(row.value) : null
	}

	/** Set a meta value. */
	setMeta(key: string, value: string): void {
		this.db
			.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)')
			.run(key, value)
	}

	/** Get the offset for a transformer + entity type pair. */
	getTransformerOffset(name: string, entityType: EntityType): number {
		const row = this.db
			.prepare('SELECT offset FROM transformer_progress WHERE name = ? AND entity_type = ?')
			.get(name, entityType) as { offset: number } | undefined
		return row?.offset ?? 0
	}

	/** Update the offset for a transformer + entity type pair. */
	setTransformerOffset(name: string, entityType: EntityType, offset: number): void {
		this.db
			.prepare(
				`INSERT INTO transformer_progress (name, entity_type, offset)
				 VALUES (?, ?, ?)
				 ON CONFLICT(name, entity_type) DO UPDATE SET offset = excluded.offset`,
			)
			.run(name, entityType, offset)
	}

	/** Check if a transformer is done for a given entity type. */
	isTransformerDone(name: string, entityType: EntityType): boolean {
		const row = this.db
			.prepare('SELECT done FROM transformer_progress WHERE name = ? AND entity_type = ?')
			.get(name, entityType) as { done: number } | undefined
		return row?.done === 1
	}

	/** Mark a transformer as done for a given entity type. */
	markTransformerDone(name: string, entityType: EntityType): void {
		this.db
			.prepare(
				`INSERT INTO transformer_progress (name, entity_type, offset, done)
				 VALUES (?, ?, 0, 1)
				 ON CONFLICT(name, entity_type) DO UPDATE SET done = 1`,
			)
			.run(name, entityType)
	}

	/** Close the database connection. */
	/**
	 * Queue a game for the org-credits step. Given involved companies replace
	 * what was queued, and reset its attempts only if they changed; null (a
	 * backfill) keeps them.
	 */
	enqueueCredits(igdbId: number, involved: unknown[] | null, now = Math.floor(Date.now() / 1000)): void {
		if (involved === null) {
			this.db.prepare('INSERT OR IGNORE INTO credit_queue (igdb_id, involved, enqueued_at) VALUES (?, NULL, ?)').run(igdbId, now)
			return
		}
		this.db.prepare(`
			INSERT INTO credit_queue (igdb_id, involved, enqueued_at) VALUES (?, ?, ?)
			ON CONFLICT (igdb_id) DO UPDATE SET
				attempts = CASE WHEN credit_queue.involved IS excluded.involved THEN credit_queue.attempts ELSE 0 END,
				last_error = CASE WHEN credit_queue.involved IS excluded.involved THEN credit_queue.last_error ELSE NULL END,
				involved = excluded.involved
		`).run(igdbId, JSON.stringify(involved), now)
	}

	/** The oldest queued games first. */
	getCreditQueue(limit: number): Array<{ igdbId: number; involved: unknown[] | null; attempts: number }> {
		const rows = this.db.prepare('SELECT igdb_id, involved, attempts FROM credit_queue ORDER BY enqueued_at, igdb_id LIMIT ?').all(limit) as Array<{ igdb_id: number; involved: string | null; attempts: number }>
		return rows.map((r) => ({ igdbId: r.igdb_id, involved: r.involved === null ? null : JSON.parse(r.involved), attempts: r.attempts }))
	}

	setCreditInvolved(igdbId: number, involved: unknown[]): void {
		this.db.prepare('UPDATE credit_queue SET involved = ? WHERE igdb_id = ?').run(JSON.stringify(involved), igdbId)
	}

	/** Count one more run this game stayed queued, and move it to the back; returns the new count. */
	bumpCreditAttempts(igdbId: number, error: string, now = Math.floor(Date.now() / 1000)): number {
		this.db.prepare('UPDATE credit_queue SET attempts = attempts + 1, last_error = ?, enqueued_at = MAX(enqueued_at + 1, ?) WHERE igdb_id = ?').run(error, now, igdbId)
		const row = this.db.prepare('SELECT attempts FROM credit_queue WHERE igdb_id = ?').get(igdbId) as { attempts: number } | undefined
		return row?.attempts ?? 0
	}

	dequeueCredits(igdbId: number): void {
		this.db.prepare('DELETE FROM credit_queue WHERE igdb_id = ?').run(igdbId)
	}

	getCreditQueueSize(): number {
		return (this.db.prepare('SELECT COUNT(*) AS n FROM credit_queue').get() as { n: number }).n
	}

	/** Record a new game held for an unmapped game_type; keeps the first held_at. */
	holdGame(igdbId: number, gameType: string | undefined, now = Math.floor(Date.now() / 1000)): void {
		this.db.prepare(`
			INSERT INTO held_games (igdb_id, game_type, held_at) VALUES (?, ?, ?)
			ON CONFLICT (igdb_id) DO UPDATE SET game_type = excluded.game_type
		`).run(igdbId, gameType ?? null, now)
	}

	releaseHeldGame(igdbId: number): void {
		this.db.prepare('DELETE FROM held_games WHERE igdb_id = ?').run(igdbId)
	}

	getHeldGames(): Array<{ igdbId: number; gameType: string | null; heldAt: number }> {
		const rows = this.db.prepare('SELECT igdb_id, game_type, held_at FROM held_games ORDER BY igdb_id').all() as Array<{ igdb_id: number; game_type: string | null; held_at: number }>
		return rows.map((r) => ({ igdbId: r.igdb_id, gameType: r.game_type, heldAt: r.held_at }))
	}

	close(): void {
		this.db.close()
	}
}
