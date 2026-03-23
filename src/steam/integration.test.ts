import { describe, it, expect } from 'vitest'
import { SteamClient } from './client.js'
import { buildSystemRequirements, mapSteamGenres, mapSteamCategories } from './mapping.js'
import { parseSteamLanguages, parseDescriptionToRichtext } from './parser.js'

describe('Steam enrichment integration', () => {
    const client = new SteamClient()

    it('should fully enrich Stardew Valley (413150)', async () => {
        const data = await client.getAppDetails(413150)
        expect(data).not.toBeNull()

        // System requirements
        const sysReqs = buildSystemRequirements(
            data!.pc_requirements,
            data!.mac_requirements,
            data!.linux_requirements,
        )
        expect(sysReqs.length).toBeGreaterThan(0)
        const pcReqs = sysReqs.find((r) => r.platform === 'windows')
        expect(pcReqs).toBeDefined()
        expect(pcReqs!.minimum).toBeDefined()
        expect(pcReqs!.minimum!.os).toBeDefined()

        // Genres
        if (data!.genres) {
            const genreResult = mapSteamGenres(data!.genres)
            expect(genreResult.genres.length + genreResult.themes.length + genreResult.modes.length).toBeGreaterThan(0)
        }

        // Categories → features
        if (data!.categories) {
            const features = mapSteamCategories(data!.categories)
            expect(features.length).toBeGreaterThan(0)
        }

        // Languages
        if (data!.supported_languages) {
            const langs = parseSteamLanguages(data!.supported_languages)
            expect(langs.length).toBeGreaterThan(0)
            expect(langs.some((l) => l.language === 'English')).toBe(true)
        }

        // Description → richtext
        if (data!.about_the_game) {
            const richtext = parseDescriptionToRichtext(data!.about_the_game)
            expect(richtext.text.length).toBeGreaterThan(0)
            expect(richtext.facets.length).toBeGreaterThan(0)
        }
    }, 30000)
})
