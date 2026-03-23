import { IGDB_GAME_TYPE_MAP } from '../igdb/types.js'
import type { Transformer } from './types.js'

interface GameWithType {
	id: number
	name: string
	game_type?: { id: number; type?: string }
}

const fixApplicationType: Transformer<GameWithType> = {
	name: 'fix-application-type',
	endpoint: 'games',
	igdbFields: 'name, game_type.type',

	transform(record, igdbItem) {
		const gameType = typeof igdbItem.game_type === 'object' ? igdbItem.game_type : undefined
		if (!gameType?.type) return false

		const newAppType = IGDB_GAME_TYPE_MAP[gameType.type]
		if (!newAppType || record.applicationType === newAppType) return false

		record.applicationType = newAppType
		return true
	},
}

export default fixApplicationType
