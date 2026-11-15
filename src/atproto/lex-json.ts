import { lexToJson } from '@atproto/api'

/**
 * A record as plain atproto JSON: blobs as `{ $type: 'blob', ref: { $link } }`
 * and links as `{ $link }`. AtpAgent responses decode these into `BlobRef` and
 * `CID` objects, which never deep-equal the plain JSON read straight from a
 * PDS and have no `ref.$link`, so everything the sync compares or inspects
 * goes through this first.
 */
export function toPlainJson<T>(value: T): T {
	if (value === undefined || value === null) return value
	return lexToJson(value as Parameters<typeof lexToJson>[0]) as T
}

/** The CID of a blob, whether it is plain JSON or a decoded `BlobRef`. */
export function blobCid(blob: unknown): string | undefined {
	if (blob === null || typeof blob !== 'object') return undefined
	const ref = (toPlainJson(blob) as { ref?: { $link?: unknown } }).ref
	return typeof ref?.$link === 'string' ? ref.$link : undefined
}
