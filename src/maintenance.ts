/**
 * Queue HappyView's scheduled maintenance jobs through the sync account's
 * session. HappyView cannot schedule jobs itself yet (HAP-187), so a
 * cartridge-infra GitHub Actions workflow runs this weekly over one SSH
 * connection, through a forced-command key that can run nothing else.
 *
 *   tsx src/maintenance.ts weekly-signals [--dry-run]
 *     syncWikidata, then refreshPublisherCounts
 *   tsx src/maintenance.ts weekly-search [--dry-run]
 *     reindexSearchGames over every game (it rewrites only what changed)
 *
 * The procedures only queue jobs, so this returns at once. A job that is
 * already pending or running is reported, not queued twice. The account must
 * be in HappyView's MAINTENANCE_DIDS script variable.
 *
 * Prints one line per procedure, and exits non-zero if any call failed.
 */
import 'dotenv/config'
import { pathToFileURL } from 'node:url'
import { ContributionClient } from './contributions/client.js'

const NS = 'games.gamesgamesgamesgames'

export const TASKS = {
	'weekly-signals': [`${NS}.syncWikidata`, `${NS}.refreshPublisherCounts`],
	'weekly-search': [`${NS}.reindexSearchGames`],
} as const

export type Task = keyof typeof TASKS

type Queuer = Pick<ContributionClient, 'queueMaintenanceJob'>

export interface CallResult {
	nsid: string
	ok: boolean
	line: string
}

/**
 * Calls each procedure of a task in order. A failure doesn't stop the rest:
 * the steps are independent, and the next week's run is the retry.
 */
export async function runTask(task: Task, dryRun: boolean, client: Queuer): Promise<CallResult[]> {
	const results: CallResult[] = []
	for (const nsid of TASKS[task]) {
		const name = nsid.slice(NS.length + 1)
		try {
			const r = await client.queueMaintenanceJob(nsid, { dryRun })
			const what = r.alreadyQueued ? `already ${r.status ?? 'queued'}` : 'queued'
			results.push({ nsid, ok: true, line: `${name}: ${what} job ${r.jobId} (dryRun=${dryRun})` })
		} catch (err) {
			results.push({ nsid, ok: false, line: `${name}: FAILED ${(err as Error).message}` })
		}
	}
	return results
}

export function parseArgs(argv: string[]): { task: Task; dryRun: boolean } | null {
	const [task, ...rest] = argv
	if (!task || !(task in TASKS)) return null
	if (rest.some((a) => a !== '--dry-run')) return null
	return { task: task as Task, dryRun: rest.includes('--dry-run') }
}

async function main() {
	const args = parseArgs(process.argv.slice(2))
	if (!args) {
		console.error(`usage: tsx src/maintenance.ts <${Object.keys(TASKS).join('|')}> [--dry-run]`)
		process.exit(2)
	}
	const { HAPPYVIEW_URL, HAPPYVIEW_CLIENT_ID, HAPPYVIEW_CLIENT_KEY, HAPPYVIEW_CLIENT_SECRET, IGDB_ATPROTO_DID } = process.env
	if (!HAPPYVIEW_URL || !HAPPYVIEW_CLIENT_ID || !HAPPYVIEW_CLIENT_KEY || !HAPPYVIEW_CLIENT_SECRET || !IGDB_ATPROTO_DID) {
		console.error('[maintenance] Missing HappyView client settings or IGDB_ATPROTO_DID in .env')
		process.exit(2)
	}
	const client = new ContributionClient({
		instanceUrl: HAPPYVIEW_URL,
		clientId: HAPPYVIEW_CLIENT_ID,
		clientKey: HAPPYVIEW_CLIENT_KEY,
		clientSecret: HAPPYVIEW_CLIENT_SECRET,
	})
	await client.restore(IGDB_ATPROTO_DID)
	const results = await runTask(args.task, args.dryRun, client)
	for (const r of results) console.log(`[maintenance] ${r.line}`)
	process.exit(results.every((r) => r.ok) ? 0 : 1)
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
	main().catch((err) => {
		console.error('[maintenance] Fatal error:', err)
		process.exit(1)
	})
}
