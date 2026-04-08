/**
 * Backfill Twitch game IDs from IGDB IDs via the Twitch Helix API.
 *
 * Queries all game records in HappyView that have an IGDB ID but no Twitch ID,
 * resolves them via the Helix API, and writes the Twitch ID back to the record
 * on the PDS via applyWrites.
 *
 * Usage: npx tsx src/backfill-twitch-ids.ts
 */

import 'dotenv/config'
import pg from 'postgres'
import { AtpAgent } from '@atproto/api'
import { countdownSleep } from './helpers.js'

const TWITCH_TOKEN_URL = 'https://id.twitch.tv/oauth2/token'
const HELIX_BASE = 'https://api.twitch.tv/helix'

// Helix allows up to 100 igdb_id params per request
const HELIX_BATCH_SIZE = 100
// Twitch rate limit: 800 requests/minute for app tokens
const REQUESTS_PER_SECOND = 10

interface TwitchGame {
	id: string
	name: string
	igdb_id: string
}

async function getTwitchToken(clientId: string, clientSecret: string): Promise<string> {
	const params = new URLSearchParams({
		client_id: clientId,
		client_secret: clientSecret,
		grant_type: 'client_credentials',
	})

	const resp = await fetch(TWITCH_TOKEN_URL, { method: 'POST', body: params })
	if (!resp.ok) {
		throw new Error(`Twitch auth failed: ${resp.status} ${await resp.text()}`)
	}

	const data = (await resp.json()) as { access_token: string }
	return data.access_token
}

async function resolveIgdbBatch(
	igdbIds: string[],
	clientId: string,
	accessToken: string,
): Promise<Map<string, string>> {
	const params = new URLSearchParams()
	for (const id of igdbIds) {
		params.append('igdb_id', id)
	}

	const resp = await fetch(`${HELIX_BASE}/games?${params}`, {
		headers: {
			'Client-ID': clientId,
			Authorization: `Bearer ${accessToken}`,
		},
	})

	if (!resp.ok) {
		if (resp.status === 429) {
			console.log('[backfill-twitch] Rate limited, waiting 60s...')
			await countdownSleep(60)
			return resolveIgdbBatch(igdbIds, clientId, accessToken)
		}
		console.error(`[backfill-twitch] Helix error: ${resp.status} ${await resp.text()}`)
		return new Map()
	}

	const body = (await resp.json()) as { data: TwitchGame[] }
	const result = new Map<string, string>()
	for (const game of body.data ?? []) {
		result.set(game.igdb_id, game.id)
	}
	return result
}

async function main() {
	const dbUrl = process.env.HAPPYVIEW_DATABASE_URL
	if (!dbUrl) throw new Error('Missing HAPPYVIEW_DATABASE_URL')

	const clientId = process.env.TWITCH_CLIENT_ID
	const clientSecret = process.env.TWITCH_CLIENT_SECRET
	if (!clientId || !clientSecret) throw new Error('Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET')

	const pdsUrl = process.env.ATPROTO_SERVICE
	const pdsId = process.env.ATPROTO_IDENTIFIER
	const pdsPw = process.env.ATPROTO_PASSWORD
	if (!pdsUrl || !pdsId || !pdsPw) throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD')

	// Authenticate
	console.log('[backfill-twitch] Authenticating with Twitch...')
	const accessToken = await getTwitchToken(clientId, clientSecret)

	console.log('[backfill-twitch] Authenticating with PDS...')
	const agent = new AtpAgent({ service: pdsUrl })
	await agent.login({ identifier: pdsId, password: pdsPw })

	// Query all games with IGDB ID but no Twitch ID
	const sql = pg(dbUrl)
	console.log('[backfill-twitch] Querying games from HappyView...')
	const allRows = await sql`
		SELECT uri, did, rkey, record
		FROM records
		WHERE collection = 'games.gamesgamesgamesgames.game'
	`
	const rows = allRows
		.map((r) => {
			const rec = typeof r.record === 'string' ? JSON.parse(r.record) : r.record
			return { ...r, record: rec, igdb_id: rec?.externalIds?.igdb as string | undefined }
		})
		.filter((r) => r.igdb_id && !r.record?.externalIds?.twitch)
	console.log(`[backfill-twitch] Found ${rows.length} games to resolve (of ${allRows.length} total)`)

	if (rows.length === 0) {
		console.log('[backfill-twitch] Nothing to do.')
		await sql.end()
		return
	}

	// Batch resolve IGDB IDs → Twitch IDs
	const igdbToTwitch = new Map<string, string>()
	const allIgdbIds = rows.map((r) => r.igdb_id!)

	for (let i = 0; i < allIgdbIds.length; i += HELIX_BATCH_SIZE) {
		const batch = allIgdbIds.slice(i, i + HELIX_BATCH_SIZE)
		const resolved = await resolveIgdbBatch(batch, clientId, accessToken)
		for (const [igdbId, twitchId] of resolved) {
			igdbToTwitch.set(igdbId, twitchId)
		}
		console.log(`[backfill-twitch] Resolved ${i + batch.length}/${allIgdbIds.length} (${igdbToTwitch.size} matched)`)

		// Rate limit
		if (i + HELIX_BATCH_SIZE < allIgdbIds.length) {
			await new Promise((r) => setTimeout(r, 1000 / REQUESTS_PER_SECOND))
		}
	}

	console.log(`[backfill-twitch] ${igdbToTwitch.size} of ${allIgdbIds.length} games have Twitch IDs`)

	// Write Twitch IDs back to PDS records via applyWrites
	const toUpdate = rows.filter((r) => igdbToTwitch.has(r.igdb_id as string))
	let updated = 0
	const WRITE_BATCH_SIZE = 200

	for (let i = 0; i < toUpdate.length; i += WRITE_BATCH_SIZE) {
		const batch = toUpdate.slice(i, i + WRITE_BATCH_SIZE)
		const writes = batch.map((row) => {
			const record = row.record as Record<string, unknown>
			const externalIds = (record.externalIds ?? {}) as Record<string, unknown>
			externalIds.twitch = igdbToTwitch.get(row.igdb_id as string)

			return {
				$type: 'com.atproto.repo.applyWrites#update' as const,
				collection: 'games.gamesgamesgamesgames.game',
				rkey: row.rkey as string,
				value: {
					...record,
					externalIds,
				},
			}
		})

		await agent.com.atproto.repo.applyWrites({
			repo: agent.session!.did,
			writes,
		})

		updated += batch.length
		console.log(`[backfill-twitch] Updated ${updated}/${toUpdate.length} records on PDS`)
	}

	console.log(`[backfill-twitch] Done. ${updated} games updated with Twitch IDs.`)
	await sql.end()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
