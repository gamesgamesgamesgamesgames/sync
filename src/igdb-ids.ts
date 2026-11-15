/**
 * `--igdb-ids <file>`: a one-shot sync of just the listed IGDB games, for
 * correcting stored records after a mapping fix.
 */

import { readFileSync } from 'node:fs'

/** The passes sync.ts runs, in order. */
export type SyncPass = 'platform' | 'game' | 'collection' | 'franchise' | 'companies' | 'orgCredits' | 'autoAcceptSweep'

const NIGHTLY_PASSES: SyncPass[] = ['platform', 'game', 'collection', 'franchise', 'companies', 'orgCredits', 'autoAcceptSweep']

/**
 * A targeted run is the game pass only. It skips the auto-accept sweep too:
 * each contribution is still auto-accepted as it is created, and the sweep
 * only retries deferrals, which the next nightly run queues anyway.
 */
export function syncPasses(igdbIds: number[] | undefined, creditIds?: number[]): Set<SyncPass> {
	if (igdbIds && creditIds) throw new Error('--igdb-ids and --backfill-credits are separate runs; pass one')
	// A credits backfill is the credits step only; it never touches games.
	if (creditIds) return new Set(['orgCredits'])
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

/** The value of `--igdb-ids <file>` or `--igdb-ids=<file>` (or another file flag), if given. */
export function igdbIdsArg(argv: string[], flag = '--igdb-ids'): string | undefined {
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i]!
		if (arg.startsWith(`${flag}=`)) return requirePath(arg.slice(flag.length + 1), flag)
		if (arg === flag) return requirePath(argv[i + 1], flag)
	}
	return undefined
}

function requirePath(path: string | undefined, flag: string): string {
	if (!path || path.startsWith('--')) throw new Error(`${flag} needs a file path`)
	return path
}

/**
 * Reads one IGDB id per line. Blank lines are ignored and duplicates
 * dropped, keeping the first. A missing or empty file, or any line that
 * isn't a positive integer, is refused.
 */
export function readIgdbIds(path: string, flag = '--igdb-ids'): number[] {
	let text: string
	try {
		text = readFileSync(path, 'utf8')
	} catch (err) {
		throw new Error(`${flag}: cannot read ${path}: ${(err as Error).message}`)
	}
	const ids = new Set<number>()
	text.split('\n').forEach((raw, i) => {
		const line = raw.trim()
		if (line === '') return
		if (!/^[1-9]\d*$/.test(line) || !Number.isSafeInteger(Number(line))) {
			throw new Error(`${flag}: ${path} line ${i + 1} is not an IGDB id: ${JSON.stringify(line.slice(0, 40))}`)
		}
		ids.add(Number(line))
	})
	if (ids.size === 0) throw new Error(`${flag}: ${path} has no ids`)
	return [...ids]
}
