import 'dotenv/config'
import { IGDBClient } from './igdb/client.js'
import { AtprotoClient } from './atproto/client.js'
import { StateManager } from './state.js'
import { mapGame } from './atproto/mapping.js'
import type { IGDBGame } from './igdb/types.js'

const igdb = new IGDBClient(process.env.TWITCH_CLIENT_ID!, process.env.TWITCH_CLIENT_SECRET!)
await igdb.authenticate()

const atproto = new AtprotoClient(process.env.ATPROTO_SERVICE!)
await atproto.login(process.env.ATPROTO_IDENTIFIER!, process.env.ATPROTO_PASSWORD!)

const state = new StateManager()

const games = await igdb.query<IGDBGame>(
	'games',
	'fields name, summary, category, game_type.type, cover.image_id; where name = "SpacePig"; limit 1;',
)

console.log('IGDB result:', JSON.stringify(games[0], null, 2))
console.log('category:', games[0]?.category)
console.log('game_type:', games[0]?.game_type)

if (games[0]) {
	const record = await mapGame(games[0], igdb, atproto, state)
	console.log('applicationType:', record.applicationType)
	console.log('Full record keys:', Object.keys(record))
}

state.close()
