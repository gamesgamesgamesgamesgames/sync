import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager, EntityType } from '../state.js'

export type IGDBEndpoint = 'games' | 'collections' | 'platforms' | 'platform_families' | 'game_engines'

export interface TransformerContext {
	igdb: IGDBClient
	atproto: AtprotoClient
	state: StateManager
}

export interface Transformer<TIGDBItem = Record<string, unknown>> {
	/** Unique key for progress tracking. */
	name: string
	/** Which IGDB endpoint to paginate. */
	endpoint: IGDBEndpoint
	/** IGDB fields fragment, e.g. "name, game_type.type" */
	igdbFields: string

	/**
	 * Transform a PDS record using IGDB data.
	 * Mutate `record` in place and return `true` if it was modified.
	 */
	transform(
		record: Record<string, unknown>,
		igdbItem: TIGDBItem,
		ctx: TransformerContext,
	): boolean | Promise<boolean>
}

interface EndpointConfig {
	entityType: EntityType
	collection: string
}

export const ENDPOINT_CONFIG: Record<IGDBEndpoint, EndpointConfig> = {
	games: {
		entityType: 'game',
		collection: 'games.gamesgamesgamesgames.game',
	},
	collections: {
		entityType: 'collection',
		collection: 'games.gamesgamesgamesgames.collection',
	},
	platforms: {
		entityType: 'platform',
		collection: 'games.gamesgamesgamesgames.platform',
	},
	platform_families: {
		entityType: 'platformFamily',
		collection: 'games.gamesgamesgamesgames.platformFamily',
	},
	game_engines: {
		entityType: 'engine',
		collection: 'games.gamesgamesgamesgames.engine',
	},
}
