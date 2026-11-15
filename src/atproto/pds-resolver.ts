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

/** The PDS could not be found or the DID could not be resolved. */
export class PdsResolutionError extends Error {
	constructor(readonly did: string, reason: string) {
		super(`could not resolve the PDS for ${did}: ${reason}`)
		this.name = 'PdsResolutionError'
	}
}

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
			response = await this.fetchImpl(url, { headers: { Accept: 'application/json' } })
		} catch (err) {
			throw new PdsResolutionError(did, `fetching ${url} failed: ${(err as Error).message}`)
		}
		if (!response.ok) {
			throw new PdsResolutionError(did, `${url} returned ${response.status}`)
		}
		const doc = await response.json() as DidDocument
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
