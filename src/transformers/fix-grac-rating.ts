import type { Transformer } from './types.js'

interface AgeRating {
	organization: string
	rating: string
	contentDescriptors?: string[]
}

const fixGracRating: Transformer = {
	name: 'fix-grac-rating',
	endpoint: 'games',
	igdbFields: 'name',

	transform(record) {
		const ageRatings = record.ageRatings as AgeRating[] | undefined
		if (!ageRatings?.length) return false

		let modified = false
		for (let i = 0; i < ageRatings.length; i++) {
			const ar = ageRatings[i]!
			if (ar.organization === 'grac' && ar.rating === 'Eighteen') {
				ageRatings[i] = { ...ar, rating: 'Nineteen' }
				modified = true
			}
		}

		return modified
	},
}

export default fixGracRating
