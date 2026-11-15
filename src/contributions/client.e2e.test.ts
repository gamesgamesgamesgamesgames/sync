/**
 * Not mocked: real HTTP against local fixture servers, through the global
 * fetch, so a wrong host or path for reading records fails here. (The
 * 2026-11-15 failure, reading records through HappyView, passed every mocked
 * test.)
 */
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { ContributionClient } from './client.js'
import { PdsResolver } from '../atproto/pds-resolver.js'
import { syncGameViaContribution } from './handler.js'

const DID = 'did:plc:fixturegames'
const GAME_URI = `at://${DID}/games.gamesgamesgamesgames.game/g1`

interface Hit { server: string; method: string; path: string }
const hits: Hit[] = []
let pds: http.Server, plc: http.Server, hv: http.Server
let pdsUrl = '', plcUrl = '', hvUrl = ''

function listen(name: string, handler: (req: http.IncomingMessage, url: URL, res: http.ServerResponse) => void): Promise<[http.Server, string]> {
	return new Promise((resolve) => {
		const server = http.createServer((req, res) => {
			const url = new URL(req.url!, 'http://localhost')
			hits.push({ server: name, method: req.method!, path: url.pathname })
			handler(req, url, res)
		})
		server.listen(0, '127.0.0.1', () => resolve([server, `http://127.0.0.1:${(server.address() as AddressInfo).port}`]))
	})
}

const send = (res: http.ServerResponse, status: number, body: unknown) => {
	res.writeHead(status, { 'content-type': 'application/json' })
	res.end(JSON.stringify(body))
}

beforeAll(async () => {
	;[pds, pdsUrl] = await listen('pds', (_req, url, res) => {
		if (url.pathname !== '/xrpc/com.atproto.repo.getRecord') return send(res, 404, { error: 'NotFound' })
		const uri = `at://${url.searchParams.get('repo')}/${url.searchParams.get('collection')}/${url.searchParams.get('rkey')}`
		if (uri === GAME_URI) return send(res, 200, { uri, cid: 'bafygame', value: { $type: 'games.gamesgamesgamesgames.game', name: 'Portal', summary: 'old' } })
		return send(res, 400, { error: 'RecordNotFound', message: `Could not locate record: ${uri}` })
	})
	;[plc, plcUrl] = await listen('plc', (_req, url, res) => {
		if (url.pathname === `/${DID}`) {
			return send(res, 200, { id: DID, service: [{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: pdsUrl }] })
		}
		send(res, 404, {})
	})
	;[hv, hvUrl] = await listen('happyview', (_req, url, res) => {
		if (url.pathname === '/xrpc/com.atproto.repo.getRecord') {
			// HappyView forwards this to the lexicon authority, which knows no repos.
			return send(res, 400, { error: 'RecordNotFound', message: 'Could not locate record' })
		}
		if (url.pathname === '/xrpc/games.gamesgamesgamesgames.getGame') {
			return send(res, 200, { game: { uri: GAME_URI, name: 'Portal', summary: 'old', releases: [{ platform: 'PC' }] } })
		}
		send(res, 404, {})
	})
})

afterAll(() => {
	pds.close(); plc.close(); hv.close()
})

function realClient() {
	return new ContributionClient({
		instanceUrl: hvUrl,
		clientId: 'https://client.test/metadata.json',
		clientKey: 'hvc_test',
		clientSecret: 'secret',
		storagePath: '/nonexistent',
		pdsResolver: new PdsResolver({ plcDirectoryUrl: plcUrl }),
		readableRepos: [DID],
		retry: { sleep: async () => {} },
	})
}

describe('reading records over real HTTP', () => {
	test('getGameRecord reads from the repo\'s PDS and never asks HappyView', async () => {
		hits.length = 0
		const rec = await realClient().getGameRecord(GAME_URI)
		expect(rec?.value).toMatchObject({ name: 'Portal', summary: 'old' })
		expect(hits.filter((h) => h.server === 'happyview')).toEqual([])
		expect(hits.map((h) => h.server)).toEqual(['plc', 'pds'])
	})

	test('a missing record is null, from the PDS', async () => {
		expect(await realClient().getGameRecord(`at://${DID}/games.gamesgamesgamesgames.game/nope`)).toBeNull()
	})

	test('a correction is diffed against the PDS record end to end', async () => {
		hits.length = 0
		const created: unknown[] = []
		const c = realClient()
		// Only createContribution is stubbed (it needs a DPoP session); the reads are real.
		;(c as unknown as { session: unknown }).session = { did: DID, fetchHandler: async () => new Response('{}') }
		c.createContribution = async (params) => { created.push(params); return { uri: 'at://x/c/1' } }
		const result = await syncGameViaContribution('602', { name: 'Portal', summary: 'new' }, c)
		expect(result.action).toBe('created')
		expect(created).toEqual([expect.objectContaining({ contributionType: 'correction', subject: GAME_URI, changes: { summary: 'new' } })])
		expect(hits.some((h) => h.server === 'happyview' && h.path === '/xrpc/com.atproto.repo.getRecord')).toBe(false)
	})
})
