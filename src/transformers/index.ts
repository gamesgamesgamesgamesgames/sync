import type { Transformer } from './types.js'
import fixApplicationType from './fix-application-type.js'

export const transformers: Transformer<any>[] = [
	fixApplicationType,
]
