/**
 * Nightly step: org.credit records for synced games.
 *
 * The game pass queues each game it maps, with the involved_companies it
 * already fetched; a backfill queues bare IGDB ids and this step fetches
 * their companies. A game is only credited once its catalogue game exists,
 * which for a new game means after auto-accept applied its newGame
 * contribution, so a game can stay queued for a few runs.
 *
 * Credits are written to the catalogue repo with the sync's catalogue
 * session, as scrapeCredits did, but with real strongRefs: `game` and `org`
 * carry the records' CIDs, and `org` points at the company's org.profile.
 *
 * Idempotent three ways: an involved company recorded in state as
 * `orgCredit` is skipped; a credit getGame already lists for the game (same
 * org URI or display name) is skipped; and the rkey is derived from the game
 * URI and company id, so a retried create can't make a second record.
 */

import { createHash } from 'node:crypto'
import type { IGDBClient } from '../igdb/client.js'
import type { AtprotoClient } from '../atproto/client.js'
import type { StateManager } from '../state.js'
import type { ContributionClient } from '../contributions/client.js'
import type { IGDBInvolvedCompany } from '../igdb/types.js'
import { mapOrgCredit } from '../atproto/mapping.js'
import { IGDB_IDS_PAGE_SIZE, breakerTrips, classifyFailure, igdbIdWhereClauses, type BreakerLimits } from './sync-entities.js'

const COLLECTION = 'games.gamesgamesgamesgames.org.credit'

const INVOLVED_FIELDS = [
	'fields involved_companies.company.id, involved_companies.company.name,',
	'involved_companies.developer, involved_companies.publisher,',
	'involved_companies.porting, involved_companies.supporting;',
].join(' ')

export interface OrgCreditsOptions {
	/** Queued games looked at per run. */
	maxGames: number
	/** Credit records created per run, to stay inside the catalogue PDS's write limits. */
	maxWrites: number
	/** Runs a game may wait (for its catalogue game or an org profile) before it is dropped. */
	maxAttempts: number
	breaker: BreakerLimits
}

export interface OrgCreditsResult {
	games: number
	written: number
	/** Credits that already existed (state or getGame). */
	existing: number
	/** Games left queued: no catalogue game yet, or a company without a profile. */
	waiting: number
	/** Games dropped after maxAttempts runs, or with no creditable companies. */
	dropped: number
	/** Games in someone else's repo, which the catalogue doesn't credit. */
	ownerHeld: number
	failed: number
	systemic: number
	remaining: number
	aborted?: string
}

/** A record key from the game URI and IGDB company id: the same credit always gets the same key. */
export function creditRkey(gameUri: string, companyId: number): string {
	return createHash('sha256').update(`${gameUri}\0${companyId}`).digest('hex').slice(0, 32)
}

function companyOf(ic: IGDBInvolvedCompany): { id?: number; name?: string } {
	return typeof ic.company === 'object' ? { id: ic.company?.id, name: ic.company?.name } : { id: ic.company }
}

interface ExistingCredit { orgUri?: string; displayName?: string; uri?: string }

class StopStep extends Error {}

export async function syncOrgCredits(
	igdb: Pick<IGDBClient, 'query'>,
	atproto: Pick<AtprotoClient, 'applyCreates' | 'getDid'>,
	contributions: Pick<ContributionClient, 'getGameByIgdbId' | 'getGameRecord'>,
	state: StateManager,
	options: OrgCreditsOptions,
): Promise<OrgCreditsResult> {
	const result: OrgCreditsResult = { games: 0, written: 0, existing: 0, waiting: 0, dropped: 0, ownerHeld: 0, failed: 0, systemic: 0, remaining: 0 }
	const catalogueDid = atproto.getDid()
	const queue = state.getCreditQueue(options.maxGames)
	console.log(`[credits] ${queue.length} of ${state.getCreditQueueSize()} queued game(s) this run (at most ${options.maxWrites} writes)`)

	// Backfilled games arrive without their companies.
	const bare = queue.filter((q) => q.involved === null)
	for (let i = 0; i < bare.length; i += IGDB_IDS_PAGE_SIZE) {
		const page = bare.slice(i, i + IGDB_IDS_PAGE_SIZE)
		const [where] = igdbIdWhereClauses(page.map((q) => q.igdbId))
		const games = await igdb.query<{ id: number; involved_companies?: IGDBInvolvedCompany[] }>('games', `${INVOLVED_FIELDS} ${where} limit ${IGDB_IDS_PAGE_SIZE};`)
		const found = new Map(games.map((g) => [g.id, g.involved_companies ?? []]))
		for (const entry of page) {
			entry.involved = found.get(entry.igdbId) ?? []
			state.setCreditInvolved(entry.igdbId, entry.involved)
		}
	}

	const orgCache = new Map<number, { uri: string; cid: string } | null>()
	const orgRef = async (companyId: number) => {
		if (orgCache.has(companyId)) return orgCache.get(companyId)!
		const uri = state.getEntity('org', companyId)
		const stored = uri ? await contributions.getGameRecord(uri) : null
		const ref = uri && stored?.cid ? { uri, cid: stored.cid } : null
		orgCache.set(companyId, ref)
		return ref
	}

	const failures: string[] = []
	let lookups = 0

	const keepWaiting = (igdbId: number, why: string) => {
		const attempts = state.bumpCreditAttempts(igdbId, why)
		if (attempts >= options.maxAttempts) {
			console.warn(`  [credits] Game ${igdbId}: ${why} after ${attempts} runs; dropping it from the queue`)
			state.dequeueCredits(igdbId)
			result.dropped++
		} else {
			result.waiting++
		}
	}

	try {
		for (const entry of queue) {
			if (result.written >= options.maxWrites) break
			result.games++
			const igdbId = entry.igdbId
			const wanted = (entry.involved as IGDBInvolvedCompany[]).filter((ic) => {
				if (state.hasEntity('orgCredit', ic.id)) { result.existing++; return false }
				return companyOf(ic).id != null && !!(ic.developer || ic.publisher || ic.porting || ic.supporting)
			})
			if (wanted.length === 0) {
				state.dequeueCredits(igdbId)
				continue
			}

			try {
				lookups++
				const game = await contributions.getGameByIgdbId(String(igdbId), { includeOrgCredits: true })
				if (!game) {
					keepWaiting(igdbId, 'no catalogue game yet')
					continue
				}
				if (!game.uri.startsWith(`at://${catalogueDid}/`)) {
					state.dequeueCredits(igdbId)
					result.ownerHeld++
					continue
				}

				const listed = (game.record.orgCredits ?? []) as ExistingCredit[]
				const todo: IGDBInvolvedCompany[] = []
				for (const ic of wanted) {
					const { id, name } = companyOf(ic)
					const orgUri = state.getEntity('org', id!)
					const match = listed.find((c) => (orgUri && c.orgUri === orgUri) || (name && c.displayName === name))
					if (match) {
						if (match.uri) state.setEntity('orgCredit', ic.id, match.uri)
						result.existing++
					} else {
						todo.push(ic)
					}
				}
				if (todo.length === 0) {
					state.dequeueCredits(igdbId)
					continue
				}
				if (result.written + todo.length > options.maxWrites) break

				const stored = await contributions.getGameRecord(game.uri)
				if (!stored?.cid) throw new Error(`getGame found ${game.uri} for IGDB ${igdbId}, but its record could not be read`)

				// One credit per company: a company IGDB lists twice on a game
				// gets one credit (one rkey) with both rows' roles.
				const writes: Array<{ ics: IGDBInvolvedCompany[]; rkey: string; record: Record<string, unknown> }> = []
				let waitingOrg = 0
				for (const ic of todo) {
					const rkey = creditRkey(game.uri, companyOf(ic).id!)
					const same = writes.find((w) => w.rkey === rkey)
					if (same) {
						const roles = (mapOrgCredit(ic, game.uri, stored.cid)?.roles ?? []) as string[]
						same.record.roles = [...new Set([...(same.record.roles as string[]), ...roles])]
						same.ics.push(ic)
						continue
					}
					const org = await orgRef(companyOf(ic).id!)
					const record = org && mapOrgCredit(ic, game.uri, stored.cid, org)
					if (record) writes.push({ ics: [ic], rkey, record })
					else waitingOrg++
				}

				if (writes.length > 0) {
					const created = await atproto.applyCreates(writes.map((w) => ({ collection: COLLECTION, rkey: w.rkey, record: w.record })))
					writes.forEach((w, i) => {
						for (const ic of w.ics) state.setEntity('orgCredit', ic.id, created[i]?.uri ?? `at://${catalogueDid}/${COLLECTION}/${w.rkey}`)
					})
					result.written += writes.length
					console.log(`  [credits] Game ${igdbId}: ${writes.length} credit(s) (${game.uri})`)
				}

				if (waitingOrg === 0) {
					state.dequeueCredits(igdbId)
				} else {
					keepWaiting(igdbId, `${waitingOrg} compan(ies) without an org profile`)
				}
			} catch (err) {
				result.failed++
				const message = (err as Error)?.message ?? String(err)
				console.error(`  [!] Credits for game ${igdbId} failed:`, message)
				if (classifyFailure(err) === 'systemic') {
					result.systemic++
					throw new StopStep(`systemic failure on game ${igdbId}: ${message}`)
				}
				keepWaiting(igdbId, message)
				failures.push(message)
				const tripped = breakerTrips(failures, lookups, options.breaker)
				if (tripped) {
					result.systemic++
					throw new StopStep(tripped)
				}
			}
		}
	} catch (err) {
		if (!(err instanceof StopStep)) throw err
		result.aborted = err.message
		console.error(`[credits] [ALERT] Stopping the credits step: ${err.message}. The rest stays queued.`)
	}

	result.remaining = state.getCreditQueueSize()
	console.log(`[credits] done (games=${result.games}, written=${result.written}, existing=${result.existing}, waiting=${result.waiting}, dropped=${result.dropped}, ownerHeld=${result.ownerHeld}, failed=${result.failed}, remaining=${result.remaining}${result.aborted ? ', aborted' : ''})`)
	return result
}
