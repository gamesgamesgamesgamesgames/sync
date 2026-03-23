import { describe, it, expect } from 'vitest'
import { SteamClient } from './client.js'

describe('SteamClient', () => {
	const client = new SteamClient()

	describe('getAppDetails', () => {
		it('should return details for TF2 (appid 440)', async () => {
			const details = await client.getAppDetails(440)

			expect(details).not.toBeNull()
			expect(details!.name).toBe('Team Fortress 2')
			expect(details!.type).toBe('game')
			expect(details!.steam_appid).toBe(440)
		}, 15_000)

		it('should return null for an invalid appid', async () => {
			const details = await client.getAppDetails(999999999)

			expect(details).toBeNull()
		}, 15_000)
	})

	describe('getItems', () => {
		it('should return store items for TF2 and Dota 2', async () => {
			const items = await client.getItems([440, 570])

			expect(items.length).toBeGreaterThanOrEqual(1)
			const appids = items.map((item) => item.appid)
			expect(appids).toContain(440)
			expect(appids).toContain(570)
		}, 15_000)
	})
})
