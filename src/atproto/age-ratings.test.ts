import { describe, test, expect } from 'vitest'
import { mapGame } from './mapping.js'
import { IGDB_AGE_RATING_CATEGORY_MAP, IGDB_LEGACY_AGE_RATING_MAP } from '../igdb/types.js'
import type { IGDBClient } from '../igdb/client.js'
import type { StateManager } from '../state.js'

// IGDB's age_rating_categories endpoint (id, organisation name, rating), as of 2026-11-15.
const IGDB_CATEGORIES: Array<[number, string, string]> = [
	[1, 'ESRB', 'RP'],
	[2, 'ESRB', 'EC'],
	[3, 'ESRB', 'E'],
	[4, 'ESRB', 'E10+'],
	[5, 'ESRB', 'T'],
	[6, 'ESRB', 'M'],
	[7, 'ESRB', 'AO'],
	[8, 'PEGI', '3'],
	[9, 'PEGI', '7'],
	[10, 'PEGI', '12'],
	[11, 'PEGI', '16'],
	[12, 'PEGI', '18'],
	[13, 'CERO', 'A'],
	[14, 'CERO', 'B'],
	[15, 'CERO', 'C'],
	[16, 'CERO', 'D'],
	[17, 'CERO', 'Z'],
	[18, 'USK', '0'],
	[19, 'USK', '6'],
	[20, 'USK', '12'],
	[21, 'USK', '16'],
	[22, 'USK', '18'],
	[23, 'GRAC', 'ALL'],
	[24, 'GRAC', '12+'],
	[25, 'GRAC', '15+'],
	[26, 'GRAC', '19+'],
	[27, 'GRAC', 'TESTING'],
	[28, 'CLASS_IND', 'L'],
	[29, 'CLASS_IND', '10'],
	[30, 'CLASS_IND', '12'],
	[31, 'CLASS_IND', '14'],
	[32, 'CLASS_IND', '16'],
	[33, 'CLASS_IND', '18'],
	[34, 'ACB', 'G'],
	[35, 'ACB', 'PG'],
	[36, 'ACB', 'M'],
	[37, 'ACB', 'MA 15+'],
	[38, 'ACB', 'R 18+'],
	[39, 'ACB', 'RC'],
	[40, 'GRAC', '18+'],
]
const ORG = { ESRB: 'esrb', PEGI: 'pegi', CERO: 'cero', USK: 'usk', GRAC: 'grac', CLASS_IND: 'classInd', ACB: 'acb' } as Record<string, string>

// lexicon search.lua's RATING_ORDER: the labels its age filters know.
const SEARCH_LABELS: Record<string, string[]> = {
	esrb: ['E', 'E10', 'T', 'M', 'AO'],
	pegi: ['Three', 'Seven', 'Twelve', 'Sixteen', 'Eighteen'],
	cero: ['A', 'B', 'C', 'D', 'Z'],
	usk: ['0', '6', '12', '16', '18'],
	grac: ['All', 'Twelve', 'Fifteen', 'Eighteen'],
	classInd: ['L', '10', '12', '14', '16', '18'],
	acb: ['G', 'PG', 'M', 'MA15', 'R18', 'X18'],
}
// Labels search.lua has no age for: pending or early-childhood ESRB, GRAC's
// test rating and 19+, and ACB's refused classification.
const NOT_IN_SEARCH = new Set(['esrb:RP', 'esrb:EC', 'grac:Testing', 'grac:Nineteen', 'acb:RC'])

const noState = { getEntity: () => undefined } as unknown as StateManager

async function ratingsOf(ageRatings: unknown[]) {
	const record = await mapGame({ id: 1, name: 'G', age_ratings: ageRatings } as never, {} as IGDBClient, {} as never, noState)
	return record.ageRatings as Array<{ organization: string; rating: string }>
}

describe('age rating categories', () => {
	test('every IGDB category maps to its own organisation', () => {
		for (const [id, org] of IGDB_CATEGORIES) {
			expect(IGDB_AGE_RATING_CATEGORY_MAP[id]?.organization, `category ${id}`).toBe(ORG[org])
		}
		expect(Object.keys(IGDB_AGE_RATING_CATEGORY_MAP).length).toBe(IGDB_CATEGORIES.length)
	})

	test('every label is one search.lua filters on, or a known exception', () => {
		for (const { organization, rating } of Object.values(IGDB_AGE_RATING_CATEGORY_MAP)) {
			const key = `${organization}:${rating}`
			if (NOT_IN_SEARCH.has(key)) continue
			expect(SEARCH_LABELS[organization], key).toContain(rating)
		}
	})

	test('each label matches what IGDB prints for it', () => {
		const words: Record<string, string> = { '3': 'Three', '7': 'Seven', '12': 'Twelve', '16': 'Sixteen', '18': 'Eighteen' }
		for (const [id, org, igdbRating] of IGDB_CATEGORIES) {
			const ours = IGDB_AGE_RATING_CATEGORY_MAP[id]!.rating
			const plain = igdbRating.replace(/[+ ]/g, '')
			const expected = org === 'PEGI' ? words[plain]
				: org === 'GRAC' ? ({ ALL: 'All', '12': 'Twelve', '15': 'Fifteen', '18': 'Eighteen', '19': 'Nineteen', TESTING: 'Testing' } as Record<string, string>)[plain]
				: plain
			expect(ours, `category ${id} (${org} ${igdbRating})`).toBe(expected)
		}
	})
})

describe('mapGame age ratings', () => {
	test('rating_category decides organisation and rating, even with no organization field', async () => {
		expect(await ratingsOf([{ id: 1, rating_category: 4 }, { id: 2, rating_category: 10 }, { id: 3, rating_category: 18 }, { id: 4, rating_category: 29 }]))
			.toEqual([
				{ organization: 'esrb', rating: 'E10' },
				{ organization: 'pegi', rating: 'Twelve' },
				{ organization: 'usk', rating: '0' },
				{ organization: 'classInd', rating: '10' },
			])
	})

	test('the old bug pairs no longer happen: category 3 with ESRB is E, not PEGI Twelve', async () => {
		expect(await ratingsOf([{ id: 1, organization: 1, rating_category: 3 }, { id: 2, organization: 2, rating_category: 9 }]))
			.toEqual([{ organization: 'esrb', rating: 'E' }, { organization: 'pegi', rating: 'Seven' }])
	})

	test('the deprecated rating enum is used only without a category, with its own numbering', async () => {
		expect(await ratingsOf([{ id: 1, rating: 3 }, { id: 2, rating: 9 }, { id: 3, rating: 20 }, { id: 4, rating: 99, rating_category: 5 }]))
			.toEqual([
				{ organization: 'pegi', rating: 'Twelve' },
				{ organization: 'esrb', rating: 'E10' },
				{ organization: 'usk', rating: '12' },
				{ organization: 'esrb', rating: 'T' },
			])
	})

	test('the legacy enum covers ids 1 to 39', () => {
		for (let id = 1; id <= 39; id++) expect(IGDB_LEGACY_AGE_RATING_MAP[id], `legacy ${id}`).toBeDefined()
		expect(IGDB_LEGACY_AGE_RATING_MAP[40]).toBeUndefined()
	})

	test('an unknown category keeps the organisation and the raw id', async () => {
		expect(await ratingsOf([{ id: 1, organization: 5, rating_category: 99 }])).toEqual([{ organization: 'grac', rating: '99' }])
	})

	test('content descriptors still come through', async () => {
		const [r] = await ratingsOf([{ id: 1, rating_category: 6, rating_content_descriptions: [{ id: 1, description: 'Blood' }] }])
		expect(r).toEqual({ organization: 'esrb', rating: 'M', contentDescriptors: ['Blood'] })
	})
})
