/**
 * One-time interactive OAuth setup for the IGDB contribution account.
 *
 * Starts a temporary HTTP server, runs the DPoP OAuth flow with the
 * user's PDS, and persists the session to disk so the cron job can
 * restore it on subsequent runs.
 *
 * Usage:
 *   npx tsx src/setup-auth.ts
 *
 * On a remote server, forward the callback port first:
 *   ssh -L 9473:127.0.0.1:9473 appview
 */

import 'dotenv/config'

import http from 'node:http'
import { ContributionClient } from './contributions/client.js'

const PORT = 9473

async function main() {
	const {
		HAPPYVIEW_URL,
		HAPPYVIEW_CLIENT_ID,
		HAPPYVIEW_CLIENT_KEY,
		HAPPYVIEW_CLIENT_SECRET,
		IGDB_ATPROTO_IDENTIFIER,
	} = process.env

	if (!HAPPYVIEW_URL || !HAPPYVIEW_CLIENT_ID || !HAPPYVIEW_CLIENT_KEY || !HAPPYVIEW_CLIENT_SECRET) {
		throw new Error(
			'Missing HAPPYVIEW_URL, HAPPYVIEW_CLIENT_ID, HAPPYVIEW_CLIENT_KEY, or HAPPYVIEW_CLIENT_SECRET in .env',
		)
	}
	if (!IGDB_ATPROTO_IDENTIFIER) {
		throw new Error('Missing IGDB_ATPROTO_IDENTIFIER in .env')
	}

	const client = new ContributionClient({
		instanceUrl: HAPPYVIEW_URL,
		clientId: HAPPYVIEW_CLIENT_ID,
		clientKey: HAPPYVIEW_CLIENT_KEY,
		clientSecret: HAPPYVIEW_CLIENT_SECRET,
	})

	const hvClient = client.oauthClient

	const server = http.createServer(async (req, res) => {
		const url = new URL(req.url!, `http://127.0.0.1:${PORT}`)

		if (url.pathname === '/oauth/callback') {
			try {
				const { session } = await hvClient.callback(url.searchParams)
				res.writeHead(200, { 'Content-Type': 'text/plain' })
				res.end(`Authenticated as ${session.did}. You can close this tab.`)
				console.log(`\nSession stored for ${session.did}`)
				console.log('Setup complete — you can now run the sync.')
				server.close()
				process.exit(0)
			} catch (err) {
				res.writeHead(500, { 'Content-Type': 'text/plain' })
				res.end(`OAuth callback failed: ${(err as Error).message}`)
				console.error('Callback error:', err)
				server.close()
				process.exit(1)
			}
			return
		}

		res.writeHead(404)
		res.end('Not found')
	})

	server.listen(PORT, '127.0.0.1', async () => {
		try {
			const authUrl = await hvClient.authorize(IGDB_ATPROTO_IDENTIFIER)
			console.log(`\nOpen this URL to authorize:\n\n  ${authUrl.href}\n`)
			console.log('Waiting for callback...')
		} catch (err) {
			console.error('Failed to start authorization:', err)
			server.close()
			process.exit(1)
		}
	})
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
