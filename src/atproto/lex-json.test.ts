import { describe, test, expect, vi } from 'vitest'
import { BlobRef, jsonToLex } from '@atproto/api'
import { AtprotoClient } from './client.js'
import { blobCid, toPlainJson } from './lex-json.js'
import { mapGame, mediaUsable } from './mapping.js'
import { diffGameRecord, pickContributableFields } from '../contributions/diff.js'
import { MirroringBlobUploader, type BlobUploader } from '../media.js'
import type { IGDBClient } from '../igdb/client.js'
import type { StateManager } from '../state.js'

const CID_A = 'bafkreie6btijqtnjfflucdqdnzzseae4dclsga3wym7enzfng3awdfpdv4'
const CID_B = 'bafkreieihvbuiieft65kkdli5ijae3iaauixzr3nialmeputbetvc6262i'

const plainBlob = (cid: string) => ({ $type: 'blob', ref: { $link: cid }, mimeType: 'image/jpeg', size: 22505 })

/** A stored game record exactly as a raw PDS read returns it. */
const storedGame = () => ({
	$type: 'games.gamesgamesgamesgames.game',
	name: 'Empire Earth III',
	media: [
		{ blob: plainBlob(CID_A), width: 600, height: 800, mediaType: 'cover', igdbImageId: 'co1' },
		{ blob: plainBlob(CID_B), width: 1920, height: 1080, mediaType: 'screenshot', igdbImageId: 'sc1' },
	],
})

/** The same record as AtpAgent hands it back: blobs decoded into BlobRef. */
const decoded = () => jsonToLex(storedGame()) as Record<string, unknown>

const igdbGame = (screenshot = 'sc1') => ({
	id: 1, name: 'Empire Earth III',
	cover: { id: 1, image_id: 'co1', width: 600, height: 800 },
	screenshots: [{ id: 2, image_id: screenshot, width: 1920, height: 1080 }],
}) as never

const noState = { getEntity: () => undefined, hasBlob: () => false, getBlob: () => undefined, setBlob: vi.fn() } as unknown as StateManager

describe('toPlainJson and blobCid', () => {
	test('an AtpAgent-decoded record goes back to its plain JSON', () => {
		expect((decoded().media as Array<{ blob: unknown }>)[0]!.blob).toBeInstanceOf(BlobRef)
		expect(toPlainJson(decoded())).toEqual(storedGame())
	})

	test('blobCid reads plain blobs and BlobRefs alike', () => {
		expect(blobCid(plainBlob(CID_A))).toBe(CID_A)
		expect(blobCid((decoded().media as Array<{ blob: unknown }>)[1]!.blob)).toBe(CID_B)
		expect(blobCid(undefined)).toBeUndefined()
		expect(blobCid({ ref: 'nope' })).toBeUndefined()
		expect(blobCid({ ref: { $link: 42 } })).toBeUndefined()
	})
})

describe('AtprotoClient.getRecord', () => {
	test('returns plain JSON, not BlobRefs', async () => {
		const client = new AtprotoClient('https://pds.invalid')
		;(client as unknown as { agent: unknown }).agent = {
			com: { atproto: { repo: { getRecord: vi.fn(async () => ({ data: { value: decoded() } })) } } },
		}
		expect(await client.getRecord('games.gamesgamesgamesgames.game', 'r1')).toEqual(storedGame())
	})
})

describe('mapGame with existing media read through AtpAgent', () => {
	test('unchanged image ids give no media change, even from BlobRef-shaped media', async () => {
		const existingMedia = decoded().media as Array<Record<string, unknown>>
		const igdb = { downloadImage: vi.fn() } as unknown as IGDBClient
		const record = await mapGame(igdbGame(), igdb, {} as never, noState, { existingMedia })
		expect(igdb.downloadImage).not.toHaveBeenCalled()
		const changes = diffGameRecord(pickContributableFields(record), storedGame())
		expect(changes).not.toHaveProperty('media')
	})

	test('a real media change still diffs', async () => {
		const existingMedia = decoded().media as Array<Record<string, unknown>>
		const igdb = { downloadImage: vi.fn(async () => Buffer.from([1])) } as unknown as IGDBClient
		const ledger = {
			isBlobMirrored: () => true, setBlobMirrored: vi.fn(),
			isBlobInContributor: () => true, setBlobInContributor: vi.fn(),
			clearBlobMirrored: vi.fn(), clearBlobInContributor: vi.fn(),
		}
		const primary: BlobUploader = { uploadBlob: vi.fn(async () => plainBlob(CID_A)) }
		const record = await mapGame(igdbGame('sc2'), igdb, {} as never, noState, {
			existingMedia,
			blobUploader: new MirroringBlobUploader(primary, { uploadBlobOnce: vi.fn(async () => plainBlob(CID_A)) }, ledger),
		})
		const changes = diffGameRecord(pickContributableFields(record), storedGame())
		expect(changes).toHaveProperty('media')
		expect((changes.media as Array<{ igdbImageId: string }>).map((m) => m.igdbImageId)).toEqual(['co1', 'sc2'])
	})
})

describe('mediaUsable', () => {
	test('checks BlobRef-shaped media by its real CIDs', () => {
		const allInContributor = vi.fn(() => true)
		const uploader = { uploadBlob: vi.fn(), allInContributor } as unknown as BlobUploader
		const tasks = [{ imageId: 'co1', mediaType: 'cover' }, { imageId: 'sc1', mediaType: 'screenshot' }] as never
		expect(mediaUsable(uploader, tasks, decoded().media as never)).toBe(true)
		expect(allInContributor).toHaveBeenCalledWith([{ imageId: 'co1', cid: CID_A }, { imageId: 'sc1', cid: CID_B }])
	})
})
