/**
 * Backfill GRAC age rating: rename "Eighteen" → "Nineteen".
 *
 * GRAC (South Korea) uses 19 as its highest age threshold, not 18.
 * The IGDB mapping incorrectly used "Eighteen" for GRAC rating_category 26.
 * This script iterates all game records and fixes any GRAC ageRating
 * entries that have rating "Eighteen" to "Nineteen".
 *
 * Usage:
 *   npx tsx src/backfill-grac-nineteen.ts
 */

import 'dotenv/config'

import { AtprotoClient } from './atproto/client.js'

const COLLECTION = 'games.gamesgamesgamesgames.game'
const BATCH_SIZE = 200

interface AgeRating {
	organization: string
	rating: string
	contentDescriptors?: string[]
}

async function main() {
	const {
		ATPROTO_SERVICE,
		ATPROTO_IDENTIFIER,
		ATPROTO_PASSWORD,
	} = process.env

	if (!ATPROTO_SERVICE || !ATPROTO_IDENTIFIER || !ATPROTO_PASSWORD) {
		throw new Error('Missing ATPROTO_SERVICE, ATPROTO_IDENTIFIER, or ATPROTO_PASSWORD in .env')
	}

	const atproto = new AtprotoClient(ATPROTO_SERVICE)
	await atproto.login(ATPROTO_IDENTIFIER, ATPROTO_PASSWORD)

	let cursor: string | undefined
	let processed = 0
	let updated = 0
	let errors = 0

	console.log('=== Backfill GRAC Eighteen → Nineteen ===')
	console.log()

	const updateBatch: Array<{ collection: string; rkey: string; record: Record<string, unknown> }> = []

	while (true) {
		const page = await atproto.listRecords(COLLECTION, 100, cursor)

		for (const record of page.records) {
			processed++

			const ageRatings = record.value.ageRatings as AgeRating[] | undefined
			if (!ageRatings?.length) continue

			const needsUpdate = ageRatings.some(
				(ar) => ar.organization === 'grac' && ar.rating === 'Eighteen',
			)
			if (!needsUpdate) continue

			const fixedRatings = ageRatings.map((ar) => {
				if (ar.organization === 'grac' && ar.rating === 'Eighteen') {
					return { ...ar, rating: 'Nineteen' }
				}
				return ar
			})

			const rkey = record.uri.split('/').pop()!
			const { $type: _$type, ...rest } = record.value
			updateBatch.push({
				collection: COLLECTION,
				rkey,
				record: { ...rest, ageRatings: fixedRatings },
			})
		}

		// Flush when batch is full or at end of pagination
		if (updateBatch.length >= BATCH_SIZE || !page.cursor) {
			while (updateBatch.length > 0) {
				const chunk = updateBatch.splice(0, BATCH_SIZE)
				try {
					await atproto.applyUpdates(chunk)
					updated += chunk.length
				} catch {
					console.warn('[warn] Batch update failed, falling back to individual writes')
					for (const write of chunk) {
						try {
							await atproto.putRecord(COLLECTION, write.rkey, write.record)
							updated++
						} catch (err) {
							console.error(`[error] putRecord failed for rkey ${write.rkey}:`, (err as Error).message)
							errors++
						}
					}
				}
			}
		}

		console.log(`[progress] processed=${processed} updated=${updated} errors=${errors}`)

		if (!page.cursor) break
		cursor = page.cursor
	}

	console.log()
	console.log('=== Backfill complete ===')
	console.log(`  Processed: ${processed}`)
	console.log(`  Updated:   ${updated}`)
	console.log(`  Errors:    ${errors}`)
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
