import type { Transformer } from './types.js'
import fixApplicationType from './fix-application-type.js'
import fixGracRating from './fix-grac-rating.js'

export const transformers: Transformer<any>[] = [
	fixApplicationType,
	fixGracRating,
]
