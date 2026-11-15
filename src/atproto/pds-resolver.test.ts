import { describe, test, expect, vi } from 'vitest'
import { PdsResolver, PdsResolutionError } from './pds-resolver.js'

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
const doc = (id: string, endpoint: string) => ({ id, service: [{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: endpoint }] })

describe('PdsResolver', () => {
	test('resolves a did:web from /.well-known/did.json', async () => {
		const fetchImpl = vi.fn(async (_u: string) => json(200, doc('did:web:gamesgamesgamesgames.games', 'https://pds.gamesgamesgamesgames.games')))
		const r = new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch })
		expect(await r.resolve('did:web:gamesgamesgamesgames.games')).toBe('https://pds.gamesgamesgamesgames.games')
		expect(fetchImpl.mock.calls[0]![0]).toBe('https://gamesgamesgamesgames.games/.well-known/did.json')
	})

	test('resolves a did:plc from the PLC directory', async () => {
		const fetchImpl = vi.fn(async (_u: string) => json(200, doc('did:plc:abc', 'https://pds.kart.sh/')))
		const r = new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch })
		expect(await r.resolve('did:plc:abc')).toBe('https://pds.kart.sh')
		expect(fetchImpl.mock.calls[0]![0]).toBe('https://plc.directory/did:plc:abc')
	})

	test('accepts the fully qualified service id', async () => {
		const fetchImpl = vi.fn(async () => json(200, { service: [{ id: 'did:plc:abc#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: 'https://p.test' }] }))
		expect(await new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch }).resolve('did:plc:abc')).toBe('https://p.test')
	})

	test('caches resolutions', async () => {
		const fetchImpl = vi.fn(async () => json(200, doc('did:plc:abc', 'https://p.test')))
		const r = new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch })
		await r.resolve('did:plc:abc'); await r.resolve('did:plc:abc')
		expect(fetchImpl).toHaveBeenCalledTimes(1)
	})

	test('picks the atproto_pds service among others', async () => {
		const fetchImpl = vi.fn(async () => json(200, { service: [
			{ id: '#atproto_labeler', type: 'AtprotoLabeler', serviceEndpoint: 'https://labeler.test' },
			{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: 'https://pds.test' },
		] }))
		expect(await new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch }).resolve('did:plc:abc')).toBe('https://pds.test')
	})

	test('a document without an atproto_pds service throws', async () => {
		const fetchImpl = vi.fn(async () => json(200, { id: 'did:plc:abc', service: [] }))
		await expect(new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch }).resolve('did:plc:abc')).rejects.toBeInstanceOf(PdsResolutionError)
	})

	test('a failed document fetch throws', async () => {
		const fetchImpl = vi.fn(async () => json(404, {}))
		await expect(new PdsResolver({ fetch: fetchImpl as unknown as typeof fetch }).resolve('did:plc:abc')).rejects.toThrow(/returned 404/)
	})

	test('unsupported DID methods and did:web paths throw', () => {
		const r = new PdsResolver()
		expect(() => r.didDocumentUrl('did:key:z6Mk')).toThrow(PdsResolutionError)
		expect(() => r.didDocumentUrl('did:web:example.com:user:alice')).toThrow(PdsResolutionError)
		expect(r.didDocumentUrl('did:web:localhost%3A8080')).toBe('https://localhost:8080/.well-known/did.json')
	})
})
