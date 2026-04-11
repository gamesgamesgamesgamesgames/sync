/**
 * Backfill slugs for game records that don't have one.
 *
 * Queries HappyView Postgres for game records missing from the slugs table,
 * generates a slug from the game name, and writes it via the putGame XRPC.
 *
 * Usage:
 *   npx tsx src/backfill-slugs.ts
 */

import 'dotenv/config'
import postgres from 'postgres'
import { slugify } from './helpers.js'

async function main() {
	const {
		HAPPYVIEW_DATABASE_URL,
		HAPPYVIEW_URL,
		HAPPYVIEW_API_KEY,
	} = process.env

	if (!HAPPYVIEW_DATABASE_URL) {
		throw new Error('Missing HAPPYVIEW_DATABASE_URL in .env')
	}
	if (!HAPPYVIEW_URL || !HAPPYVIEW_API_KEY) {
		throw new Error('Missing HAPPYVIEW_URL or HAPPYVIEW_API_KEY in .env')
	}

	const sql = postgres(HAPPYVIEW_DATABASE_URL)

	console.log('Finding game records without slugs...')

	const allGames = await sql`
		SELECT r.uri, r.record
		FROM records r
		LEFT JOIN slugs s ON s.uri = r.uri
		WHERE r.collection = 'games.gamesgamesgamesgames.game'
		  AND s.uri IS NULL
		ORDER BY r.uri
	`
	const games = allGames
		.map((r) => {
			const rec = typeof r.record === 'string' ? JSON.parse(r.record) : r.record
			return { uri: r.uri, name: rec?.name as string | undefined }
		})
		.filter((r) => r.name)

	console.log(`Found ${games.length} games without slugs.`)

	if (games.length === 0) {
		await sql.end()
		return
	}

	// Build a set of existing slugs to detect collisions
	const existingSlugs = await sql`SELECT slug FROM slugs`
	const usedSlugs = new Set(existingSlugs.map((r) => r.slug as string))

	let created = 0
	let errors = 0

	for (const game of games) {
		const name = game.name as string
		const uri = game.uri as string
		let slug = slugify(name)

		if (!slug) {
			console.warn(`  [!] Could not generate slug for "${name}" (${uri})`)
			errors++
			continue
		}

		// Handle collisions by appending a counter
		if (usedSlugs.has(slug)) {
			let counter = 2
			while (usedSlugs.has(`${slug}-${counter}`)) {
				counter++
			}
			slug = `${slug}-${counter}`
		}

		try {
			const response = await fetch(`${HAPPYVIEW_URL}/xrpc/games.gamesgamesgamesgames.putGame`, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'Authorization': `Bearer ${HAPPYVIEW_API_KEY}`,
				},
				body: JSON.stringify({ uri, slug }),
			})

			if (!response.ok) {
				const body = await response.text()
				throw new Error(`putGame returned ${response.status}: ${body}`)
			}

			usedSlugs.add(slug)
			created++

			if (created % 100 === 0) {
				console.log(`  [backfill] Progress: ${created}/${games.length}`)
			}
		} catch (err) {
			console.error(`  [!] Failed to set slug for "${name}" (${uri}):`, (err as Error).message)
			errors++
		}
	}

	console.log()
	console.log('=== Slug backfill complete ===')
	console.log(`  Created: ${created}`)
	console.log(`  Errors: ${errors}`)
	console.log(`  Total without slugs: ${games.length}`)

	await sql.end()
}

main().catch((err) => {
	console.error('Fatal error:', err)
	process.exit(1)
})
