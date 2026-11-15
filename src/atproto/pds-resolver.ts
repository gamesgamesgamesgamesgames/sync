/**
 * Find the PDS that hosts a repo, from its DID document.
 *
 * Raw records have to be read from the repo's own PDS. HappyView does not
 * serve com.atproto.repo.getRecord itself: on this instance it forwards the
 * call to the lexicon authority, which answers 400 RecordNotFound for every
 * record (that made every existing game unreadable on 2026-11-15).
 */

export interface PdsResolverOptions {
	/** Defaults to https://plc.directory. */
	plcDirectoryUrl?: string
	/** Injectable for tests. Defaults to the global fetch. */
	fetch?: typeof fetch
	/** How long a resolution is cached (default 1 hour). */
	ttlMs?: number
}

/**
 * The PDS could not be found or the DID could not be resolved.
 * `transient` is true for transport trouble (network, timeout, 5xx, 429),
 * which hits every repo alike; false when the DID itself is the problem
 * (404 or 410, a tombstone, no atproto_pds service, an unsupported form).
 */
export class PdsResolutionError extends Error {
	constructor(readonly did: string, reason: string, readonly transient = false) {
		super(`could not resolve the PDS for ${did}: ${reason}`)
		this.name = 'PdsResolutionError'
	}
}

/** Fetch options for reads from hosts named by DID documents. */
export const SAFE_READ_TIMEOUT_MS = 15_000

/**
 * Read a response body as text, refusing more than `maxBytes`. The hosts we
 * read from come from DID documents, so a response's size is not ours to
 * trust.
 */
export async function readCapped(response: Response, maxBytes: number): Promise<string> {
	const declared = Number(response.headers.get('content-length'))
	if (Number.isFinite(declared) && declared > maxBytes) {
		await response.body?.cancel().catch(() => undefined)
		throw new Error(`response of ${declared} bytes exceeds the ${maxBytes}-byte cap`)
	}
	if (!response.body) return ''
	const reader = response.body.getReader()
	const chunks: Uint8Array[] = []
	let total = 0
	for (;;) {
		const { done, value } = await reader.read()
		if (done) break
		total += value.byteLength
		if (total > maxBytes) {
			await reader.cancel().catch(() => undefined)
			throw new Error(`response exceeds the ${maxBytes}-byte cap`)
		}
		chunks.push(value)
	}
	return Buffer.concat(chunks).toString('utf8')
}

const MAX_DID_DOCUMENT_BYTES = 256 * 1024

interface DidDocument {
	id?: string
	service?: Array<{ id?: string; type?: string; serviceEndpoint?: unknown }>
}

export class PdsResolver {
	private plcDirectoryUrl: string
	private fetchImpl: typeof fetch
	private ttlMs: number
	private cache = new Map<string, { endpoint: string; expiresAt: number }>()

	constructor(options: PdsResolverOptions = {}) {
		this.plcDirectoryUrl = (options.plcDirectoryUrl ?? 'https://plc.directory').replace(/\/+$/, '')
		this.fetchImpl = options.fetch ?? ((...args) => fetch(...args))
		this.ttlMs = options.ttlMs ?? 3_600_000
	}

	/** The DID document URL for a did:plc or did:web. */
	didDocumentUrl(did: string): string {
		if (did.startsWith('did:plc:')) {
			return `${this.plcDirectoryUrl}/${did}`
		}
		if (did.startsWith('did:web:')) {
			// did:web:example.com → https://example.com/.well-known/did.json.
			// Only bare hosts are supported, which covers every repo we read.
			const host = decodeURIComponent(did.slice('did:web:'.length))
			if (!host || (host.includes(':') && !/^[^:]+:\d+$/.test(host))) {
				throw new PdsResolutionError(did, 'unsupported did:web form')
			}
			return `https://${host}/.well-known/did.json`
		}
		throw new PdsResolutionError(did, 'unsupported DID method')
	}

	async resolve(did: string): Promise<string> {
		const cached = this.cache.get(did)
		if (cached && cached.expiresAt > Date.now()) return cached.endpoint

		const url = this.didDocumentUrl(did)
		let response: Response
		try {
			response = await this.fetchImpl(url, {
				headers: { Accept: 'application/json' },
				redirect: 'error',
				signal: AbortSignal.timeout(SAFE_READ_TIMEOUT_MS),
			})
		} catch (err) {
			throw new PdsResolutionError(did, `fetching ${url} failed: ${(err as Error).message}`, true)
		}
		if (!response.ok) {
			const transient = response.status >= 500 || response.status === 429
			await response.body?.cancel().catch(() => undefined)
			throw new PdsResolutionError(did, `${url} returned ${response.status}`, transient)
		}
		let doc: DidDocument
		try {
			doc = JSON.parse(await readCapped(response, MAX_DID_DOCUMENT_BYTES)) as DidDocument
		} catch (err) {
			throw new PdsResolutionError(did, `unreadable DID document: ${(err as Error).message}`)
		}
		const service = (doc.service ?? []).find((s) =>
			(s.id === '#atproto_pds' || s.id === `${did}#atproto_pds`) && s.type === 'AtprotoPersonalDataServer')
		const endpoint = typeof service?.serviceEndpoint === 'string' ? service.serviceEndpoint.replace(/\/+$/, '') : undefined
		if (!endpoint || !/^https?:\/\//.test(endpoint)) {
			throw new PdsResolutionError(did, 'the DID document has no atproto_pds service')
		}
		this.cache.set(did, { endpoint, expiresAt: Date.now() + this.ttlMs })
		return endpoint
	}
}
