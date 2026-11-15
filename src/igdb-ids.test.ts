import { afterEach, beforeEach, describe, expect, test } from 'vitest'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { assertStateForIgdbIds, igdbIdsArg, readIgdbIds, syncPasses } from './igdb-ids.js'

describe('syncPasses', () => {
	test('a targeted run is the game pass only, with no sweep', () => {
		expect([...syncPasses([1, 2])]).toEqual(['game'])
	})

	test('the nightly run is unchanged', () => {
		expect([...syncPasses(undefined)]).toEqual(['platform', 'game', 'collection', 'franchise', 'companies', 'orgCredits', 'autoAcceptSweep'])
	})

	test('a credits backfill is the credits step only', () => {
		expect([...syncPasses(undefined, [1])]).toEqual(['orgCredits'])
	})

	test('--igdb-ids and --backfill-credits are refused together', () => {
		expect(() => syncPasses([1], [2])).toThrow(/separate runs/)
	})
})

describe('igdbIdsArg', () => {
	test('takes the path in either form', () => {
		expect(igdbIdsArg(['--igdb-ids', 'ids.txt'])).toBe('ids.txt')
		expect(igdbIdsArg(['--igdb-ids=ids.txt'])).toBe('ids.txt')
	})

	test('is undefined without the flag', () => {
		expect(igdbIdsArg([])).toBeUndefined()
		expect(igdbIdsArg(['--phase2-only'])).toBeUndefined()
	})

	test('reads another file flag the same way', () => {
		expect(igdbIdsArg(['--backfill-credits', 'c.txt'], '--backfill-credits')).toBe('c.txt')
		expect(igdbIdsArg(['--igdb-ids', 'i.txt'], '--backfill-credits')).toBeUndefined()
		expect(() => igdbIdsArg(['--backfill-credits'], '--backfill-credits')).toThrow(/--backfill-credits needs a file path/)
	})

	test('refuses the flag without a path', () => {
		expect(() => igdbIdsArg(['--igdb-ids'])).toThrow(/needs a file path/)
		expect(() => igdbIdsArg(['--igdb-ids='])).toThrow(/needs a file path/)
		expect(() => igdbIdsArg(['--igdb-ids', '--other'])).toThrow(/needs a file path/)
	})
})

describe('readIgdbIds', () => {
	let dir: string
	beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'igdb-ids-')) })
	afterEach(() => { rmSync(dir, { recursive: true, force: true }) })

	const file = (text: string) => {
		const path = join(dir, 'ids.txt')
		writeFileSync(path, text)
		return path
	}

	test('reads one id per line, ignoring blank lines and duplicates', () => {
		expect(readIgdbIds(file('1942\n  7346 \r\n\n1942\n119171\n'))).toEqual([1942, 7346, 119171])
	})

	test('refuses a missing file', () => {
		expect(() => readIgdbIds(join(dir, 'nope.txt'))).toThrow(/cannot read/)
	})

	test('refuses an empty file', () => {
		expect(() => readIgdbIds(file(''))).toThrow(/has no ids/)
		expect(() => readIgdbIds(file('\n \n'))).toThrow(/has no ids/)
	})

	test.each(['abc', '0', '-5', '1.5', '12 34', '1e3', 'id', '99999999999999999999'])('refuses a bad id: %s', (bad) => {
		expect(() => readIgdbIds(file(`1\n${bad}\n3\n`))).toThrow(/line 2 is not an IGDB id/)
	})
})

describe('assertStateForIgdbIds', () => {
	test('refuses a state DB with no game cursor', () => {
		expect(() => assertStateForIgdbIds({ getLastSyncAt: () => null })).toThrow(/no last_sync_game/)
	})

	test('accepts one a nightly run has built', () => {
		expect(() => assertStateForIgdbIds({ getLastSyncAt: () => 1_781_308_800 })).not.toThrow()
	})
})
