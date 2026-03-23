import { XRPCError } from '@atproto/xrpc'

/** Return a service label prefix based on the error type. */
export function errorLabel(err: unknown): string {
	if (err instanceof XRPCError) return '[atproto]'
	const msg = (err as Error).message ?? ''
	if (msg.includes('IGDB') || msg.includes('igdb') || msg.includes('Image download')) return '[igdb]'
	return '[error]'
}

/** Format a millisecond duration as a human-friendly string (e.g. "2.5s", "1m 30s"). */
export function formatWait(ms: number): string {
	if (ms < 1000) return `${ms}ms`
	const totalSeconds = Math.round(ms / 1000)
	if (totalSeconds < 60) return `${totalSeconds}s`
	const minutes = Math.floor(totalSeconds / 60)
	const seconds = totalSeconds % 60
	return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`
}

/**
 * Sleep for `ms` milliseconds while displaying a live countdown on the
 * current terminal line. The line is overwritten in place every second so
 * there is always a visible "time remaining" without log spam.
 *
 * @param ms - Total time to sleep in milliseconds
 * @param label - Prefix shown before the countdown (e.g. "[atproto] Rate limited")
 */
/**
 * Prefetch wrapper: fetches the next page from an async generator while the
 * caller processes the current batch, keeping one page buffered ahead.
 */
export async function* prefetch<T>(
	source: AsyncGenerator<T>,
): AsyncGenerator<T> {
	let nextPromise = source.next()

	while (true) {
		const result = await nextPromise
		if (result.done) break

		// Start fetching the next page immediately
		nextPromise = source.next()

		yield result.value
	}
}

/** Whether stdout supports TTY control sequences (false when redirected to a file). */
const isTTY = typeof process.stdout.clearLine === 'function'

export function countdownSleep(ms: number, label: string): Promise<void> {
	if (!isTTY) {
		// Non-interactive: log countdown every second
		return new Promise((resolve) => {
			const endAt = Date.now() + ms
			const tick = () => {
				const remaining = Math.max(endAt - Date.now(), 0)
				if (remaining <= 0) {
					resolve()
				} else {
					console.log(`${label} — ${formatWait(remaining)} remaining...`)
					setTimeout(tick, 1000)
				}
			}
			tick()
		})
	}

	return new Promise((resolve) => {
		const endAt = Date.now() + ms

		const write = () => {
			const remaining = Math.max(endAt - Date.now(), 0)
			process.stdout.write(`\r${label} — ${formatWait(remaining)} remaining...`)
		}

		write()
		const interval = setInterval(() => {
			const remaining = endAt - Date.now()
			if (remaining <= 0) {
				clearInterval(interval)
				process.stdout.clearLine(0)
				process.stdout.cursorTo(0)
				resolve()
			} else {
				write()
			}
		}, 1000)

		// Ensure we resolve even if the interval undershoots
		setTimeout(() => {
			clearInterval(interval)
			process.stdout.clearLine(0)
			process.stdout.cursorTo(0)
			resolve()
		}, ms)
	})
}
