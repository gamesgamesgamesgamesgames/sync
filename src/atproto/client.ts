/**
 * atproto client for creating records and uploading blobs.
 *
 * Self-hosted PDS — no client-side rate limiting needed.
 * applyWrites: up to 200 operations per call.
 */

import { AtpAgent } from '@atproto/api'
import { XRPCError } from '@atproto/xrpc'
import { countdownSleep } from '../helpers.js'

export class AtprotoClient {
	private agent: AtpAgent
	private did: string = ''
	private totalRecordCount: number = 0

	constructor(service: string) {
		this.agent = new AtpAgent({ service })
	}

	/** Login to the PDS (retries on transient server errors). */
	async login(identifier: string, password: string): Promise<void> {
		const result = await this.callWithRetry(
			() => this.agent.login({ identifier, password }),
			'login',
		)
		this.did = result.data.did
		console.log(`[atproto] Logged in as ${this.did}`)
	}

	/** Get the DID of the authenticated user. */
	getDid(): string {
		return this.did
	}

	/** Get the current access JWT for authenticating with external services. */
	getAccessJwt(): string | undefined {
		return this.agent.session?.accessJwt
	}

	/** Get the total number of records created this session. */
	getTotalRecordCount(): number {
		return this.totalRecordCount
	}

	/** Retry a function with exponential backoff for transient errors. */
	private async callWithRetry<T>(fn: () => Promise<T>, label: string): Promise<T> {
		const maxRetries = 5

		for (let attempt = 0; attempt <= maxRetries; attempt++) {
			try {
				return await fn()
			} catch (err) {
				const isLastAttempt = attempt === maxRetries

				if (err instanceof XRPCError) {
					if (err.status === 429) {
						if (isLastAttempt) throw err
						const resetHeader = err.headers?.['ratelimit-reset']
						const waitMs = resetHeader
							? Math.max((Number(resetHeader) * 1000) - Date.now(), 1000)
							: 2000 * (attempt + 1)
						await countdownSleep(waitMs, `[atproto] Rate limited on ${label}`)
						continue
					}

					if (err.status >= 500) {
						if (isLastAttempt) throw err
						const waitMs = 1000 * Math.pow(2, attempt)
						await countdownSleep(waitMs, `[atproto] ${label} server error (${err.status}), retrying`)
						continue
					}

					// 4xx client errors (not 429) — don't retry
					throw err
				}

				// Network errors (e.g. TypeError: fetch failed)
				if (err instanceof TypeError) {
					if (isLastAttempt) throw err
					const waitMs = 1000 * Math.pow(2, attempt)
					await countdownSleep(waitMs, `[atproto] ${label} network error, retrying`)
					continue
				}

				throw err
			}
		}

		// Should be unreachable, but satisfies TypeScript
		throw new Error(`[atproto] ${label} failed after ${maxRetries} retries`)
	}

	/** Upload a blob (image) to the PDS. */
	async uploadBlob(
		data: Uint8Array,
		mimeType: string,
	): Promise<{ ref: { $link: string }; mimeType: string; size: number }> {
		return this.callWithRetry(async () => {
			const response = await this.agent.uploadBlob(data, { encoding: mimeType })

			return {
				ref: { $link: response.data.blob.ref.toString() },
				mimeType: response.data.blob.mimeType,
				size: response.data.blob.size,
			}
		}, 'uploadBlob')
	}

	/** Create a record in the authenticated user's repo. */
	async createRecord(
		collection: string,
		record: Record<string, unknown>,
		rkey?: string,
	): Promise<{ uri: string; cid: string }> {
		return this.callWithRetry(async () => {
			this.totalRecordCount++

			const response = await this.agent.com.atproto.repo.createRecord({
				repo: this.did,
				collection,
				rkey,
				record: {
					$type: collection,
					...record,
				},
			})

			return {
				uri: response.data.uri,
				cid: response.data.cid,
			}
		}, 'createRecord')
	}

	/** List records in a collection, with optional pagination. */
	async listRecords(
		collection: string,
		limit: number = 100,
		cursor?: string,
	): Promise<{ records: Array<{ uri: string; cid: string; value: Record<string, unknown> }>; cursor?: string }> {
		return this.callWithRetry(async () => {
			const response = await this.agent.com.atproto.repo.listRecords({
				repo: this.did,
				collection,
				limit,
				cursor,
			})

			return {
				records: response.data.records.map((r) => ({
					uri: r.uri,
					cid: r.cid,
					value: r.value as Record<string, unknown>,
				})),
				cursor: response.data.cursor,
			}
		}, 'listRecords')
	}

	/** Get a record by collection and rkey. Returns null if not found. */
	async getRecord(
		collection: string,
		rkey: string,
	): Promise<Record<string, unknown> | null> {
		try {
			return await this.callWithRetry(async () => {
				const response = await this.agent.com.atproto.repo.getRecord({
					repo: this.did,
					collection,
					rkey,
				})
				return response.data.value as Record<string, unknown>
			}, 'getRecord')
		} catch (err) {
			if (err instanceof XRPCError && err.status === 404) {
				return null
			}
			throw err
		}
	}

	/** Put (upsert) a record at a specific rkey. */
	async putRecord(
		collection: string,
		rkey: string,
		record: Record<string, unknown>,
	): Promise<{ uri: string; cid: string }> {
		return this.callWithRetry(async () => {
			this.totalRecordCount++

			const response = await this.agent.com.atproto.repo.putRecord({
				repo: this.did,
				collection,
				rkey,
				record: {
					$type: collection,
					...record,
				},
			})

			return {
				uri: response.data.uri,
				cid: response.data.cid,
			}
		}, 'putRecord')
	}

	/** Batch-create records in a single applyWrites call (up to 200). */
	async applyCreates(
		writes: Array<{ collection: string; record: Record<string, unknown>; rkey?: string }>,
	): Promise<Array<{ uri: string; cid: string }>> {
		return this.callWithRetry(async () => {
			this.totalRecordCount += writes.length

			const response = await this.agent.com.atproto.repo.applyWrites({
				repo: this.did,
				writes: writes.map((w) => ({
					$type: 'com.atproto.repo.applyWrites#create',
					collection: w.collection,
					rkey: w.rkey,
					value: {
						$type: w.collection,
						...w.record,
					},
				})),
			})

			const results = (response.data as { results?: Array<{ uri: string; cid: string }> }).results ?? []
			return results.map((r) => ({ uri: r.uri, cid: r.cid }))
		}, 'applyCreates')
	}

	/** Batch-delete records in a single applyWrites call (up to 200). */
	async applyDeletes(
		writes: Array<{ collection: string; rkey: string }>,
	): Promise<void> {
		await this.callWithRetry(async () => {
			await this.agent.com.atproto.repo.applyWrites({
				repo: this.did,
				writes: writes.map((w) => ({
					$type: 'com.atproto.repo.applyWrites#delete',
					collection: w.collection,
					rkey: w.rkey,
				})),
			})
		}, 'applyDeletes')
	}

	/** Batch-update records in a single applyWrites call (up to 200). */
	async applyUpdates(
		writes: Array<{ collection: string; rkey: string; record: Record<string, unknown> }>,
	): Promise<Array<{ uri: string; cid: string }>> {
		return this.callWithRetry(async () => {
			this.totalRecordCount += writes.length

			const response = await this.agent.com.atproto.repo.applyWrites({
				repo: this.did,
				writes: writes.map((w) => ({
					$type: 'com.atproto.repo.applyWrites#update',
					collection: w.collection,
					rkey: w.rkey,
					value: {
						$type: w.collection,
						...w.record,
					},
				})),
			})

			const results = (response.data as { results?: Array<{ uri: string; cid: string }> }).results ?? []
			return results.map((r) => ({ uri: r.uri, cid: r.cid }))
		}, 'applyUpdates')
	}
}
