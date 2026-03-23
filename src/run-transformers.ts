/**
 * Run all pending transformers.
 *
 * Transformers are small, focused modules that fix or update existing
 * PDS records using IGDB data. The runner handles pagination, batching,
 * offset tracking, and graceful shutdown.
 *
 * Usage:
 *   npx tsx src/run-transformers.ts
 */

import 'dotenv/config'

import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { runTransformers } from './transformers/runner.js'

async function main() {
	const {
		TWITCH_CLIENT_ID,
		TWITCH_CLIENT_SECRET,
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
	} = process.env

	if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
		throw new Error('Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET in .env')
	}
	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD in .env')
	}

	const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
	await igdb.authenticate()

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	const state = new StateManager()

	try {
		await runTransformers(igdb, atproto, state)
	} finally {
		state.close()
	}
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
