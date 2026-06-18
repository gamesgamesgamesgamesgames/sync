import { readFile, writeFile, unlink, mkdir } from 'node:fs/promises'
import { dirname, join } from 'node:path'

const DEFAULT_DIR = join(process.cwd(), '.happyview-sessions')

export class FileStorage {
	private dir: string

	constructor(dir?: string) {
		this.dir = dir ?? DEFAULT_DIR
	}

	private path(key: string): string {
		const safe = key.replace(/[^a-zA-Z0-9_:-]/g, '_')
		return join(this.dir, `${safe}.json`)
	}

	async get(key: string): Promise<string | null> {
		try {
			return await readFile(this.path(key), 'utf-8')
		} catch {
			return null
		}
	}

	async set(key: string, value: string): Promise<void> {
		await mkdir(dirname(this.path(key)), { recursive: true })
		await writeFile(this.path(key), value, 'utf-8')
	}

	async delete(key: string): Promise<void> {
		try {
			await unlink(this.path(key))
		} catch {
			// ignore missing
		}
	}
}
