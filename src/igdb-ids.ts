/**
 * `--igdb-ids <file>`: a one-shot sync of just the listed IGDB games, for
 * correcting stored records after a mapping fix.
 */

import { readFileSync } from 'node:fs'

/** The passes sync.ts runs, in order. */
export type SyncPass = 'platform' | 'game' | 'collection' | 'franchise' | 'companies' | 'autoAcceptSweep'

const NIGHTLY_PASSES: SyncPass[] = ['platform', 'game', 'collection', 'franchise', 'companies', 'autoAcceptSweep']

/**
 * A targeted run is the game pass only. It skips the auto-accept sweep too:
 * each contribution is still auto-accepted as it is created, and the sweep
 * only retries deferrals, which the next nightly run queues anyway.
 */
export function syncPasses(igdbIds: number[] | undefined): Set<SyncPass> {
	return new Set(igdbIds ? ['game'] : NIGHTLY_PASSES)
}

/**
 * A targeted run needs the state DB a nightly run has built: without it
 * every game looks new and its references to platforms, companies and the
 * rest can't be resolved. No game cursor means no such run, which usually
 * means the wrong working directory (STATE_FILE is relative).
 */
export function assertStateForIgdbIds(state: { getLastSyncAt(type: 'game'): number | null }): void {
	if (!state.getLastSyncAt('game')) {
		throw new Error(`--igdb-ids: the state DB in ${process.cwd()} has no last_sync_game; run from the sync's own directory, after a nightly run`)
	}
}

/** The value of `--igdb-ids <file>` or `--igdb-ids=<file>`, if given. */
export function igdbIdsArg(argv: string[]): string | undefined {
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i]!
		if (arg.startsWith('--igdb-ids=')) return requirePath(arg.slice('--igdb-ids='.length))
		if (arg === '--igdb-ids') return requirePath(argv[i + 1])
	}
	return undefined
}

function requirePath(path: string | undefined): string {
	if (!path || path.startsWith('--')) throw new Error('--igdb-ids needs a file path')
	return path
}

/**
 * Reads one IGDB id per line. Blank lines are ignored and duplicates
 * dropped, keeping the first. A missing or empty file, or any line that
 * isn't a positive integer, is refused.
 */
export function readIgdbIds(path: string): number[] {
	let text: string
	try {
		text = readFileSync(path, 'utf8')
	} catch (err) {
		throw new Error(`--igdb-ids: cannot read ${path}: ${(err as Error).message}`)
	}
	const ids = new Set<number>()
	text.split('\n').forEach((raw, i) => {
		const line = raw.trim()
		if (line === '') return
		if (!/^[1-9]\d*$/.test(line) || !Number.isSafeInteger(Number(line))) {
			throw new Error(`--igdb-ids: ${path} line ${i + 1} is not an IGDB id: ${JSON.stringify(line.slice(0, 40))}`)
		}
		ids.add(Number(line))
	})
	if (ids.size === 0) throw new Error(`--igdb-ids: ${path} has no ids`)
	return [...ids]
}
