import { describe, it, expect } from 'vitest'
import {
	mapSteamGenres,
	mapSteamCategories,
	mergeLanguageSupports,
	buildSystemRequirements,
} from './mapping.js'

// ---------------------------------------------------------------------------
// mapSteamGenres
// ---------------------------------------------------------------------------

describe('mapSteamGenres', () => {
	it('should map known genres to the correct fields', () => {
		const result = mapSteamGenres([
			{ id: '25', description: 'Adventure' },
			{ id: '23', description: 'Indie' },
			{ id: '3', description: 'RPG' },
			{ id: '28', description: 'Simulation' },
			{ id: '18', description: 'Sports' },
			{ id: '2', description: 'Strategy' },
			{ id: '4', description: 'Casual' },
			{ id: '9', description: 'Racing' },
		])

		expect(result.genres).toEqual([
			'adventure',
			'indie',
			'rpg',
			'simulator',
			'sport',
			'strategy',
			'casual',
			'racing',
		])
		expect(result.themes).toEqual([])
		expect(result.modes).toEqual([])
	})

	it('should route Action to themes', () => {
		const result = mapSteamGenres([
			{ id: '1', description: 'Action' },
		])

		expect(result.themes).toEqual(['action'])
		expect(result.genres).toEqual([])
		expect(result.modes).toEqual([])
	})

	it('should route Massively Multiplayer to modes', () => {
		const result = mapSteamGenres([
			{ id: '29', description: 'Massively Multiplayer' },
		])

		expect(result.modes).toEqual(['mmo'])
		expect(result.genres).toEqual([])
		expect(result.themes).toEqual([])
	})

	it('should skip Free to Play and other non-game genres', () => {
		const result = mapSteamGenres([
			{ id: '37', description: 'Free to Play' },
			{ id: '70', description: 'Early Access' },
			{ id: '57', description: 'Utilities' },
			{ id: '51', description: 'Design & Illustration' },
			{ id: '25', description: 'Adventure' },
		])

		expect(result.genres).toEqual(['adventure'])
		expect(result.themes).toEqual([])
		expect(result.modes).toEqual([])
	})

	it('should convert unknown genres to camelCase and add to genres', () => {
		const result = mapSteamGenres([
			{ id: '99', description: 'Puzzle Platformer' },
		])

		expect(result.genres).toEqual(['puzzlePlatformer'])
	})

	it('should handle mixed genres across all three fields', () => {
		const result = mapSteamGenres([
			{ id: '1', description: 'Action' },
			{ id: '25', description: 'Adventure' },
			{ id: '29', description: 'Massively Multiplayer' },
		])

		expect(result.genres).toEqual(['adventure'])
		expect(result.themes).toEqual(['action'])
		expect(result.modes).toEqual(['mmo'])
	})
})

// ---------------------------------------------------------------------------
// mapSteamCategories
// ---------------------------------------------------------------------------

describe('mapSteamCategories', () => {
	it('should map known category IDs to feature strings', () => {
		const result = mapSteamCategories([
			{ id: 2, description: 'Single-player' },
			{ id: 22, description: 'Steam Achievements' },
			{ id: 23, description: 'Steam Cloud' },
			{ id: 29, description: 'Steam Trading Cards' },
		])

		expect(result).toEqual([
			'singlePlayer',
			'achievements',
			'cloudSaves',
			'steamTradingCards',
		])
	})

	it('should deduplicate features', () => {
		const result = mapSteamCategories([
			{ id: 2, description: 'Single-player' },
			{ id: 2, description: 'Single-player' },
			{ id: 22, description: 'Steam Achievements' },
		])

		expect(result).toEqual(['singlePlayer', 'achievements'])
	})

	it('should skip unknown category IDs', () => {
		const result = mapSteamCategories([
			{ id: 2, description: 'Single-player' },
			{ id: 999, description: 'Unknown Category' },
		])

		expect(result).toEqual(['singlePlayer'])
	})

	it('should return empty array for empty input', () => {
		expect(mapSteamCategories([])).toEqual([])
	})

	it('should map multiplayer and coop categories', () => {
		const result = mapSteamCategories([
			{ id: 1, description: 'Multi-player' },
			{ id: 9, description: 'Co-op' },
			{ id: 36, description: 'Online Multi-Player' },
			{ id: 38, description: 'Online Co-op' },
			{ id: 27, description: 'Cross-Platform Multiplayer' },
		])

		expect(result).toEqual([
			'multiPlayer',
			'coop',
			'multiPlayerOnline',
			'coopOnline',
			'crossPlatformMultiplayer',
		])
	})
})

// ---------------------------------------------------------------------------
// mergeLanguageSupports
// ---------------------------------------------------------------------------

describe('mergeLanguageSupports', () => {
	it('should give IGDB entries precedence over Steam entries', () => {
		const igdb = [
			{ language: 'English', audio: true, subtitles: true, interface: true },
		]
		const steam = [
			{ language: 'English', audio: false },
		]

		const result = mergeLanguageSupports(igdb, steam)

		expect(result).toHaveLength(1)
		expect(result[0]).toEqual({
			language: 'English',
			audio: true,
			subtitles: true,
			interface: true,
		})
	})

	it('should add Steam-only languages with defaults', () => {
		const igdb = [
			{ language: 'English', audio: true, subtitles: true, interface: true },
		]
		const steam = [
			{ language: 'English', audio: true },
			{ language: 'French', audio: false },
			{ language: 'German', audio: true },
		]

		const result = mergeLanguageSupports(igdb, steam)

		expect(result).toHaveLength(3)
		expect(result[0]).toEqual({
			language: 'English',
			audio: true,
			subtitles: true,
			interface: true,
		})
		expect(result[1]).toEqual({
			language: 'French',
			audio: false,
			subtitles: true,
			interface: true,
		})
		expect(result[2]).toEqual({
			language: 'German',
			audio: true,
			subtitles: true,
			interface: true,
		})
	})

	it('should handle empty IGDB data', () => {
		const result = mergeLanguageSupports(
			[],
			[
				{ language: 'English', audio: true },
				{ language: 'Japanese', audio: false },
			],
		)

		expect(result).toHaveLength(2)
		expect(result[0]).toEqual({
			language: 'English',
			audio: true,
			subtitles: true,
			interface: true,
		})
		expect(result[1]).toEqual({
			language: 'Japanese',
			audio: false,
			subtitles: true,
			interface: true,
		})
	})

	it('should handle empty Steam data', () => {
		const igdb = [
			{ language: 'English', audio: true, subtitles: true, interface: true },
		]

		const result = mergeLanguageSupports(igdb, [])

		expect(result).toHaveLength(1)
		expect(result[0]).toEqual(igdb[0])
	})
})

// ---------------------------------------------------------------------------
// buildSystemRequirements
// ---------------------------------------------------------------------------

describe('buildSystemRequirements', () => {
	it('should build requirements from Steam HTML', () => {
		const pcReqs = {
			minimum: `<ul class="bb_ul">
				<li><strong>OS:</strong> Windows 10<br></li>
				<li><strong>Processor:</strong> Intel Core i5<br></li>
				<li><strong>Memory:</strong> 8 GB RAM<br></li>
			</ul>`,
			recommended: `<ul class="bb_ul">
				<li><strong>OS:</strong> Windows 11<br></li>
				<li><strong>Processor:</strong> Intel Core i7<br></li>
				<li><strong>Memory:</strong> 16 GB RAM<br></li>
			</ul>`,
		}

		const result = buildSystemRequirements(pcReqs, [], [])

		expect(result).toHaveLength(1)
		expect(result[0]!.platform).toBe('windows')
		expect(result[0]!.minimum).toEqual({
			os: 'Windows 10',
			processor: 'Intel Core i5',
			memory: '8 GB RAM',
		})
		expect(result[0]!.recommended).toEqual({
			os: 'Windows 11',
			processor: 'Intel Core i7',
			memory: '16 GB RAM',
		})
	})

	it('should skip empty platform requirements (empty arrays)', () => {
		const pcReqs = {
			minimum: `<ul class="bb_ul">
				<li><strong>OS:</strong> Windows 10<br></li>
			</ul>`,
		}

		const result = buildSystemRequirements(pcReqs, [], [])

		expect(result).toHaveLength(1)
		expect(result[0]!.platform).toBe('windows')
	})

	it('should handle multiple platforms', () => {
		const pcReqs = {
			minimum: `<ul class="bb_ul"><li><strong>OS:</strong> Windows 10<br></li></ul>`,
		}
		const macReqs = {
			minimum: `<ul class="bb_ul"><li><strong>OS:</strong> macOS 12<br></li></ul>`,
		}
		const linuxReqs = {
			minimum: `<ul class="bb_ul"><li><strong>OS:</strong> Ubuntu 20.04<br></li></ul>`,
		}

		const result = buildSystemRequirements(pcReqs, macReqs, linuxReqs)

		expect(result).toHaveLength(3)
		expect(result[0]!.platform).toBe('windows')
		expect(result[1]!.platform).toBe('mac')
		expect(result[2]!.platform).toBe('linux')
	})

	it('should skip platforms where both specs parse to empty', () => {
		const pcReqs = {
			minimum: '<p>No structured data here</p>',
			recommended: '<p>Also no structured data</p>',
		}

		const result = buildSystemRequirements(pcReqs, [], [])

		expect(result).toHaveLength(0)
	})

	it('should omit recommended if only minimum is present', () => {
		const pcReqs = {
			minimum: `<ul class="bb_ul"><li><strong>OS:</strong> Windows 10<br></li></ul>`,
		}

		const result = buildSystemRequirements(pcReqs, [], [])

		expect(result).toHaveLength(1)
		expect(result[0]!.minimum).toBeDefined()
		expect(result[0]!.recommended).toBeUndefined()
	})
})
