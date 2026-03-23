import { describe, it, expect } from 'vitest'
import {
	parseSystemRequirements,
	parseSteamLanguages,
	parseDescriptionToRichtext,
} from './parser.js'

// ---------------------------------------------------------------------------
// parseSystemRequirements
// ---------------------------------------------------------------------------

describe('parseSystemRequirements', () => {
	it('should parse standard Steam requirements HTML', () => {
		const html = `<ul class="bb_ul">
			<li><strong>OS:</strong> Windows 10<br></li>
			<li><strong>Processor:</strong> Intel Core i7-4770K<br></li>
			<li><strong>Memory:</strong> 8 GB RAM<br></li>
			<li><strong>Graphics:</strong> NVIDIA GTX 970<br></li>
			<li><strong>DirectX:</strong> Version 11<br></li>
			<li><strong>Storage:</strong> 50 GB available space<br></li>
		</ul>`

		const spec = parseSystemRequirements(html)

		expect(spec.os).toBe('Windows 10')
		expect(spec.processor).toBe('Intel Core i7-4770K')
		expect(spec.memory).toBe('8 GB RAM')
		expect(spec.graphics).toBe('NVIDIA GTX 970')
		expect(spec.directx).toBe('Version 11')
		expect(spec.storage).toBe('50 GB available space')
	})

	it('should parse old-style labels (OS *, Hard Disk Space, Video Card, Sound)', () => {
		const html = `<ul class="bb_ul">
			<li><strong>OS *:</strong> Windows 7<br></li>
			<li><strong>CPU:</strong> AMD FX-8350<br></li>
			<li><strong>RAM:</strong> 4 GB<br></li>
			<li><strong>Video Card:</strong> AMD Radeon R9 280<br></li>
			<li><strong>Hard Disk Space:</strong> 20 GB<br></li>
			<li><strong>Sound:</strong> DirectX compatible<br></li>
		</ul>`

		const spec = parseSystemRequirements(html)

		expect(spec.os).toBe('Windows 7')
		expect(spec.processor).toBe('AMD FX-8350')
		expect(spec.memory).toBe('4 GB')
		expect(spec.graphics).toBe('AMD Radeon R9 280')
		expect(spec.storage).toBe('20 GB')
		expect(spec.soundCard).toBe('DirectX compatible')
	})

	it('should return empty object for empty/undefined input', () => {
		expect(parseSystemRequirements('')).toEqual({})
		expect(parseSystemRequirements(undefined)).toEqual({})
	})

	it('should parse Additional Notes field', () => {
		const html = `<ul class="bb_ul">
			<li><strong>OS:</strong> Windows 10<br></li>
			<li><strong>Additional Notes:</strong> Requires internet connection<br></li>
		</ul>`

		const spec = parseSystemRequirements(html)

		expect(spec.os).toBe('Windows 10')
		expect(spec.additionalNotes).toBe('Requires internet connection')
	})

	it('should map Network label to additionalNotes', () => {
		const html = `<ul class="bb_ul">
			<li><strong>Network:</strong> Broadband Internet connection<br></li>
		</ul>`

		const spec = parseSystemRequirements(html)
		expect(spec.additionalNotes).toBe('Broadband Internet connection')
	})
})

// ---------------------------------------------------------------------------
// parseSteamLanguages
// ---------------------------------------------------------------------------

describe('parseSteamLanguages', () => {
	it('should parse standard language string with one audio language', () => {
		const html = 'English<strong>*</strong>, French, German'

		const result = parseSteamLanguages(html)

		expect(result).toEqual([
			{ language: 'English', audio: true },
			{ language: 'French', audio: false },
			{ language: 'German', audio: false },
		])
	})

	it('should parse multiple audio languages', () => {
		const html =
			'English<strong>*</strong>, French<strong>*</strong>, German<br><strong>*</strong>languages with full audio support'

		const result = parseSteamLanguages(html)

		expect(result).toEqual([
			{ language: 'English', audio: true },
			{ language: 'French', audio: true },
			{ language: 'German', audio: false },
		])
	})

	it('should return empty array for empty input', () => {
		expect(parseSteamLanguages('')).toEqual([])
		expect(parseSteamLanguages(undefined)).toEqual([])
	})
})

// ---------------------------------------------------------------------------
// parseDescriptionToRichtext
// ---------------------------------------------------------------------------

describe('parseDescriptionToRichtext', () => {
	const NS = 'games.gamesgamesgamesgames.richtext.facet'

	it('should convert bold text to a bold facet', () => {
		const html = 'Hello <strong>world</strong>!'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Hello world!')
		expect(result.facets).toHaveLength(1)
		expect(result.facets[0]!.features[0]!.$type).toBe(`${NS}#bold`)

		const start = result.facets[0]!.index.byteStart
		const end = result.facets[0]!.index.byteEnd
		expect(result.text.slice(start, end)).toBe('world')
	})

	it('should convert <b> to a bold facet', () => {
		const html = '<b>bold</b> text'
		const result = parseDescriptionToRichtext(html)

		expect(result.facets).toHaveLength(1)
		expect(result.facets[0]!.features[0]!.$type).toBe(`${NS}#bold`)
	})

	it('should convert italic text to an italic facet', () => {
		const html = '<em>emphasis</em>'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('emphasis')
		expect(result.facets).toHaveLength(1)
		expect(result.facets[0]!.features[0]!.$type).toBe(`${NS}#italic`)
	})

	it('should convert <i> to an italic facet', () => {
		const html = '<i>italic</i>'
		const result = parseDescriptionToRichtext(html)

		expect(result.facets).toHaveLength(1)
		expect(result.facets[0]!.features[0]!.$type).toBe(`${NS}#italic`)
	})

	it('should convert headings to heading facets with level', () => {
		const html = '<h2>Title</h2>Some text'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Title\nSome text')

		const headingFacet = result.facets.find(
			(f) => f.features[0]!.$type === `${NS}#heading`,
		)
		expect(headingFacet).toBeDefined()
		expect(headingFacet!.features[0]!.level).toBe(2)

		const start = headingFacet!.index.byteStart
		const end = headingFacet!.index.byteEnd
		expect(result.text.slice(start, end)).toBe('Title')
	})

	it('should convert links to link facets with uri', () => {
		const html = 'Visit <a href="https://store.steampowered.com">Steam</a> now'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Visit Steam now')

		const linkFacet = result.facets.find(
			(f) => f.features[0]!.$type === `${NS}#link`,
		)
		expect(linkFacet).toBeDefined()
		expect(linkFacet!.features[0]!.uri).toBe(
			'https://store.steampowered.com',
		)

		const start = linkFacet!.index.byteStart
		const end = linkFacet!.index.byteEnd
		expect(result.text.slice(start, end)).toBe('Steam')
	})

	it('should convert list items to listItem facets', () => {
		const html = '<ul><li>Item one</li><li>Item two</li></ul>'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Item one\nItem two')

		const listFacets = result.facets.filter(
			(f) => f.features[0]!.$type === `${NS}#listItem`,
		)
		expect(listFacets).toHaveLength(2)
		expect(listFacets[0]!.features[0]!.ordered).toBe(false)
		expect(listFacets[0]!.features[0]!.depth).toBe(0)
	})

	it('should convert <br> tags to newlines', () => {
		const html = 'Line 1<br>Line 2<br/>Line 3'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Line 1\nLine 2\nLine 3')
	})

	it('should strip images and videos', () => {
		const html =
			'Before<img src="test.png">After<video><source src="test.mp4"></video>End'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('BeforeAfterEnd')
	})

	it('should strip bb_img_ctn spans and their contents', () => {
		const html =
			'Before<span class="bb_img_ctn">image content here</span>After'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('BeforeAfter')
	})

	it('should return empty result for empty input', () => {
		const result = parseDescriptionToRichtext('')
		expect(result.text).toBe('')
		expect(result.facets).toEqual([])

		const result2 = parseDescriptionToRichtext(undefined)
		expect(result2.text).toBe('')
		expect(result2.facets).toEqual([])
	})

	it('should decode HTML entities', () => {
		const html = 'Rock &amp; Roll &lt;3'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Rock & Roll <3')
	})

	it('should collapse 3+ consecutive newlines to 2', () => {
		const html = 'A<br><br><br><br>B'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('A\n\nB')
	})

	it('should handle </p> as newline', () => {
		const html = '<p>Paragraph one</p><p>Paragraph two</p>'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('Paragraph one\nParagraph two')
	})

	it('should handle UTF-8 byte offsets correctly', () => {
		// "cafe" with accented e is 2 bytes in UTF-8
		const html = 'caf\u00e9 <strong>bold</strong>'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('caf\u00e9 bold')

		// "cafe " is 4 chars but "caf\u00e9 " is 6 bytes (e-acute = 2 bytes)
		const boldFacet = result.facets[0]!
		expect(boldFacet.index.byteStart).toBe(6) // "caf\u00e9 " = 6 bytes
		expect(boldFacet.index.byteEnd).toBe(10) // "bold" = 4 bytes
	})

	it('should strip script and style tags with content', () => {
		const html =
			'Before<script>alert("xss")</script>Middle<style>.foo{color:red}</style>After'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('BeforeMiddleAfter')
	})

	it('should strip iframe tags', () => {
		const html =
			'Before<iframe src="https://example.com">content</iframe>After'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('BeforeAfter')
	})

	it('should handle numeric HTML entities', () => {
		const html = '&#169; 2024 &#x2014; All rights reserved'
		const result = parseDescriptionToRichtext(html)

		expect(result.text).toBe('\u00A9 2024 \u2014 All rights reserved')
	})
})
