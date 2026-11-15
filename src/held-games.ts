/**
 * New games held back because their IGDB game_type doesn't map to an
 * applicationType. The cursor moves past them, so they are kept in state and
 * re-checked each run until the map knows their type.
 */

import type { StateManager } from './state.js'
import type { ContributionSyncResult } from './contributions/handler.js'
import { IGDB_GAME_TYPE_MAP } from './igdb/types.js'

/** Held games whose game_type maps now, so a run can propose them. */
export function heldGamesToRecheck(
	held: Array<{ igdbId: number; gameType: string | null }>,
	typeMap: Record<string, string> = IGDB_GAME_TYPE_MAP,
): number[] {
	return held.filter((h) => h.gameType !== null && typeMap[h.gameType] !== undefined).map((h) => h.igdbId)
}

/**
 * Keeps held_games in step with the game pass: a held game is recorded with
 * its game_type, and any other outcome releases it.
 */
export function trackHeldGames(state: StateManager, gameTypeOf: (key: string) => string | undefined) {
	return (key: string, action: ContributionSyncResult['action']) => {
		if (action === 'held') state.holdGame(Number(key), gameTypeOf(key))
		else state.releaseHeldGame(Number(key))
	}
}

/** The needs-attention line and the rerun list for games still held, or null. */
export function heldGamesReport(held: Array<{ igdbId: number; gameType: string | null }>): { attention: string; rerun: string } | null {
	if (held.length === 0) return null
	const types = [...new Set(held.map((h) => h.gameType ?? '(none)'))].sort()
	return {
		attention: `${held.length} new game(s) held for unmapped IGDB game_type(s) ${types.map((t) => JSON.stringify(t)).join(', ')}; add them to IGDB_GAME_TYPE_MAP`,
		rerun: held.map((h) => h.igdbId).join('\n'),
	}
}
