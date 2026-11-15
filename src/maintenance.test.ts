import { describe, expect, test, vi } from 'vitest'
import { parseArgs, runTask } from './maintenance.js'

describe('parseArgs', () => {
	test('accepts the two tasks, with or without --dry-run', () => {
		expect(parseArgs(['weekly-signals'])).toEqual({ task: 'weekly-signals', dryRun: false })
		expect(parseArgs(['weekly-search', '--dry-run'])).toEqual({ task: 'weekly-search', dryRun: true })
	})

	test('refuses anything else', () => {
		expect(parseArgs([])).toBeNull()
		expect(parseArgs(['reindex-everything'])).toBeNull()
		expect(parseArgs(['weekly-signals', '--force'])).toBeNull()
	})
})

describe('runTask', () => {
	test('weekly-signals syncs Wikidata, then refreshes publisher counts', async () => {
		const calls: string[] = []
		const client = { queueMaintenanceJob: vi.fn(async (nsid: string, input: Record<string, unknown>) => {
			calls.push(`${nsid} ${JSON.stringify(input)}`)
			return { jobId: `job-${calls.length}`, alreadyQueued: false }
		}) }
		const results = await runTask('weekly-signals', true, client)
		expect(calls).toEqual([
			'games.gamesgamesgamesgames.syncWikidata {"dryRun":true}',
			'games.gamesgamesgamesgames.refreshPublisherCounts {"dryRun":true}',
		])
		expect(results.every((r) => r.ok)).toBe(true)
		expect(results[0].line).toBe('syncWikidata: queued job job-1 (dryRun=true)')
	})

	test('weekly-search queues a full search reindex', async () => {
		const client = { queueMaintenanceJob: vi.fn(async () => ({ jobId: 'job-9', alreadyQueued: true, status: 'running' })) }
		const results = await runTask('weekly-search', false, client)
		expect(client.queueMaintenanceJob).toHaveBeenCalledWith('games.gamesgamesgamesgames.reindexSearchGames', { dryRun: false })
		expect(results[0].line).toBe('reindexSearchGames: already running job job-9 (dryRun=false)')
	})

	test('a failure is reported and the next procedure still runs', async () => {
		const client = { queueMaintenanceJob: vi.fn()
			.mockRejectedValueOnce(new Error('syncWikidata returned 400: unauthorized'))
			.mockResolvedValueOnce({ jobId: 'job-2', alreadyQueued: false }) }
		const results = await runTask('weekly-signals', false, client)
		expect(results.map((r) => r.ok)).toEqual([false, true])
		expect(results[0].line).toContain('FAILED syncWikidata returned 400')
	})
})
