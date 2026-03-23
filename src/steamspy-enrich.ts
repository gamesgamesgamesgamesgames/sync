import 'dotenv/config'

const HAPPYVIEW_URL = process.env.HAPPYVIEW_URL
const HAPPYVIEW_API_KEY = process.env.HAPPYVIEW_API_KEY

if (!HAPPYVIEW_URL) {
	console.error('[steamspy-enrich] HAPPYVIEW_URL is required')
	process.exit(1)
}

if (!HAPPYVIEW_API_KEY) {
	console.error('[steamspy-enrich] HAPPYVIEW_API_KEY is required')
	process.exit(1)
}

const STEAMSPY_URL = 'https://steamspy.com/api.php?request=top100in2weeks'

type SteamSpyGame = {
	appid: number
	ccu: number
	[key: string]: unknown
}

async function fetchTopGames(): Promise<SteamSpyGame[]> {
	const response = await fetch(STEAMSPY_URL)
	if (!response.ok) {
		throw new Error(`SteamSpy API returned ${response.status}`)
	}
	const data = await response.json() as Record<string, SteamSpyGame>
	return Object.values(data)
}

async function upsertPopularity(games: SteamSpyGame[]) {
	if (games.length === 0) return 0

	const payload = {
		games: games.map((g) => ({
			steamId: String(g.appid),
			ccu: g.ccu,
		})),
	}

	const response = await fetch(`${HAPPYVIEW_URL}/xrpc/games.gamesgamesgamesgames.putPopularity`, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'Authorization': `Bearer ${HAPPYVIEW_API_KEY}`,
		},
		body: JSON.stringify(payload),
	})

	if (!response.ok) {
		const body = await response.text()
		throw new Error(`putPopularity returned ${response.status}: ${body}`)
	}

	const result = await response.json() as { upserted: number }
	return result.upserted
}

async function main() {
	console.log('[steamspy-enrich] Starting SteamSpy enrichment run')

	console.log('[steamspy-enrich] Fetching top games from SteamSpy...')
	const games = await fetchTopGames()
	console.log(`[steamspy-enrich] Got ${games.length} games, upserting via XRPC...`)

	const upserted = await upsertPopularity(games)
	console.log(`[steamspy-enrich] Done — upserted ${upserted} popularity entries`)
}

main().catch((err) => {
	console.error('[steamspy-enrich] Fatal error:', err)
	process.exit(1)
})
