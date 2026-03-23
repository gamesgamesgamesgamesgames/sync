/**
 * Bounded concurrency pool.
 *
 * Limits how many async tasks run simultaneously using a semaphore-style
 * queue with acquire()/release().
 */

export class ConcurrencyPool {
	private running = 0
	private waiters: Array<() => void> = []

	constructor(private readonly limit: number) {}

	private acquire(): Promise<void> {
		if (this.running < this.limit) {
			this.running++
			return Promise.resolve()
		}
		return new Promise<void>((resolve) => {
			this.waiters.push(resolve)
		})
	}

	private release(): void {
		const next = this.waiters.shift()
		if (next) {
			next()
		} else {
			this.running--
		}
	}

	/** Run an async function within the pool's concurrency limit. */
	async run<T>(fn: () => Promise<T>): Promise<T> {
		await this.acquire()
		try {
			return await fn()
		} finally {
			this.release()
		}
	}
}
