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

			CREATE TABLE IF NOT EXISTS steam_map (
				igdb_id TEXT PRIMARY KEY,
				steam_id TEXT NOT NULL,
				at_uri TEXT NOT NULL,
				enriched_at INTEGER
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
	close(): void {
		this.db.close()
	}
}
