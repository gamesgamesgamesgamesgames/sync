// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

export interface SystemSpec {
	os?: string
	processor?: string
	memory?: string
	graphics?: string
	directx?: string
	storage?: string
	soundCard?: string
	additionalNotes?: string
}

export interface SteamLanguage {
	language: string
	audio: boolean
}

export interface RichtextFacet {
	index: { byteStart: number; byteEnd: number }
	features: Array<{ $type: string; [key: string]: unknown }>
}

export interface RichtextResult {
	text: string
	facets: RichtextFacet[]
}

// ---------------------------------------------------------------------------
// Label mappings for system requirements
// ---------------------------------------------------------------------------

const LABEL_MAP: Record<string, keyof SystemSpec> = {
	os: 'os',
	'os *': 'os',
	processor: 'processor',
	cpu: 'processor',
	memory: 'memory',
	ram: 'memory',
	graphics: 'graphics',
	'video card': 'graphics',
	video: 'graphics',
	gpu: 'graphics',
	directx: 'directx',
	storage: 'storage',
	'hard disk space': 'storage',
	'hard drive': 'storage',
	'sound card': 'soundCard',
	sound: 'soundCard',
	'additional notes': 'additionalNotes',
	network: 'additionalNotes',
}

// ---------------------------------------------------------------------------
// HTML entity decoding
// ---------------------------------------------------------------------------

const NAMED_ENTITIES: Record<string, string> = {
	'&amp;': '&',
	'&lt;': '<',
	'&gt;': '>',
	'&quot;': '"',
	'&#39;': "'",
	'&apos;': "'",
	'&nbsp;': ' ',
}

function decodeHtmlEntities(str: string): string {
	return str
		.replace(
			/&(?:amp|lt|gt|quot|apos|nbsp|#39);/gi,
			(match) => NAMED_ENTITIES[match.toLowerCase()] ?? match,
		)
		.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) =>
			String.fromCodePoint(parseInt(hex, 16)),
		)
		.replace(/&#(\d+);/g, (_, dec) =>
			String.fromCodePoint(parseInt(dec, 10)),
		)
}

// ---------------------------------------------------------------------------
// Strip HTML tags helper
// ---------------------------------------------------------------------------

function stripTags(html: string): string {
	return html.replace(/<[^>]*>/g, '')
}

// ---------------------------------------------------------------------------
// 1. parseSystemRequirements
// ---------------------------------------------------------------------------

export function parseSystemRequirements(html: string | undefined): SystemSpec {
	if (!html) return {}

	const spec: SystemSpec = {}

	// Match <li> items containing <strong>Label:</strong> Value
	const liRegex = /<li[^>]*>([\s\S]*?)<\/li>/gi
	let liMatch: RegExpExecArray | null

	while ((liMatch = liRegex.exec(html)) !== null) {
		const content = liMatch[1]!
		// Extract label from <strong>Label:</strong>
		const strongMatch = content.match(
			/<strong>\s*(.*?)\s*:?\s*<\/strong>\s*([\s\S]*)/i,
		)
		if (!strongMatch) continue

		const rawLabel = stripTags(strongMatch[1]!).trim().replace(/:$/, '')
		const rawValue = stripTags(strongMatch[2]!).trim()

		const key = LABEL_MAP[rawLabel.toLowerCase()]
		if (key && rawValue) {
			spec[key] = decodeHtmlEntities(rawValue)
		}
	}

	return spec
}

// ---------------------------------------------------------------------------
// 2. parseSteamLanguages
// ---------------------------------------------------------------------------

export function parseSteamLanguages(
	html: string | undefined,
): SteamLanguage[] {
	if (!html) return []

	// Remove footnote text after <br> (e.g., "<br><strong>*</strong>languages with full audio support")
	const cleaned = html.replace(/<br\s*\/?>[\s\S]*$/i, '')

	// Split by comma
	const parts = cleaned.split(',')

	return parts
		.map((part) => {
			const trimmed = part.trim()
			if (!trimmed) return null

			const audio = trimmed.includes('<strong>*</strong>')
			const language = stripTags(trimmed).replace(/\*/, '').trim()

			if (!language) return null
			return { language, audio }
		})
		.filter((entry): entry is SteamLanguage => entry !== null)
}

// ---------------------------------------------------------------------------
// 3. parseDescriptionToRichtext
// ---------------------------------------------------------------------------

const FACET_NS = 'games.gamesgamesgamesgames.richtext.facet'

// Tags whose content should be completely stripped
const STRIP_CONTENT_TAGS = new Set([
	'img',
	'video',
	'source',
	'iframe',
	'script',
	'style',
])

function byteLength(str: string): number {
	return Buffer.byteLength(str, 'utf-8')
}

function byteOffset(text: string, charIndex: number): number {
	return byteLength(text.slice(0, charIndex))
}

interface TagInfo {
	tag: string
	attrs: Record<string, string>
	selfClosing: boolean
	isClose: boolean
	className?: string
}

function parseTag(tagStr: string): TagInfo {
	const isClose = tagStr.startsWith('</')
	const selfClosing = tagStr.endsWith('/>') || /^<(br|img|source|hr)\b/i.test(tagStr)

	// Extract tag name
	const nameMatch = tagStr.match(/<\/?(\w+)/)
	const tag = nameMatch ? nameMatch[1]!.toLowerCase() : ''

	// Extract attributes
	const attrs: Record<string, string> = {}
	const attrRegex = /(\w[\w-]*)=(?:"([^"]*)"|'([^']*)')/g
	let attrMatch: RegExpExecArray | null
	while ((attrMatch = attrRegex.exec(tagStr)) !== null) {
		attrs[attrMatch[1]!.toLowerCase()] = attrMatch[2] ?? attrMatch[3] ?? ''
	}

	return {
		tag,
		attrs,
		selfClosing,
		isClose,
		className: attrs['class'],
	}
}

export function parseDescriptionToRichtext(
	html: string | undefined,
): RichtextResult {
	if (!html) return { text: '', facets: [] }

	let text = ''
	const facets: RichtextFacet[] = []

	// Stack for tracking open tags and their text start positions
	const stack: Array<{
		tag: string
		attrs: Record<string, string>
		textStart: number
	}> = []

	// Track whether we are inside a tag whose content should be stripped
	let stripDepth = 0
	// Track bb_img_ctn spans
	let bbImgCtnDepth = 0

	// Tokenize: split into tags and text segments
	const tokenRegex = /(<[^>]+>)/g
	const tokens = html.split(tokenRegex)

	for (const token of tokens) {
		if (!token) continue

		// Is this a tag?
		if (token.startsWith('<')) {
			const info = parseTag(token)

			// Handle bb_img_ctn spans (strip content)
			if (
				info.tag === 'span' &&
				info.className?.includes('bb_img_ctn')
			) {
				bbImgCtnDepth++
				continue
			}
			if (info.tag === 'span' && info.isClose && bbImgCtnDepth > 0) {
				bbImgCtnDepth--
				continue
			}

			// Skip content inside stripped containers
			if (bbImgCtnDepth > 0) continue

			// Handle content-stripping tags
			if (STRIP_CONTENT_TAGS.has(info.tag)) {
				if (info.selfClosing) continue
				if (info.isClose) {
					stripDepth = Math.max(0, stripDepth - 1)
				} else {
					stripDepth++
				}
				continue
			}

			if (stripDepth > 0) continue

			// <br> → newline
			if (info.tag === 'br') {
				text += '\n'
				continue
			}

			if (info.isClose) {
				// Find the matching open tag on the stack
				let matched = -1
				for (let i = stack.length - 1; i >= 0; i--) {
					if (stack[i]!.tag === info.tag) {
						matched = i
						break
					}
				}

				if (matched >= 0) {
					const opened = stack.splice(matched, 1)[0]!
					const startChar = opened.textStart
					const endChar = text.length

					// Only create a facet if there's actual content
					if (endChar > startChar) {
						const bStart = byteOffset(text, startChar)
						const bEnd = byteOffset(text, endChar)

						switch (info.tag) {
							case 'strong':
							case 'b':
								facets.push({
									index: {
										byteStart: bStart,
										byteEnd: bEnd,
									},
									features: [
										{ $type: `${FACET_NS}#bold` },
									],
								})
								break

							case 'em':
							case 'i':
								facets.push({
									index: {
										byteStart: bStart,
										byteEnd: bEnd,
									},
									features: [
										{ $type: `${FACET_NS}#italic` },
									],
								})
								break

							case 'h1':
							case 'h2':
							case 'h3':
							case 'h4':
							case 'h5':
							case 'h6':
								facets.push({
									index: {
										byteStart: bStart,
										byteEnd: bEnd,
									},
									features: [
										{
											$type: `${FACET_NS}#heading`,
											level: parseInt(
												info.tag.charAt(1),
												10,
											),
										},
									],
								})
								break

							case 'a':
								if (opened.attrs['href']) {
									facets.push({
										index: {
											byteStart: bStart,
											byteEnd: bEnd,
										},
										features: [
											{
												$type: `${FACET_NS}#link`,
												uri: opened.attrs['href'],
											},
										],
									})
								}
								break

							case 'li':
								facets.push({
									index: {
										byteStart: bStart,
										byteEnd: bEnd,
									},
									features: [
										{
											$type: `${FACET_NS}#listItem`,
											ordered: false,
											depth: 0,
										},
									],
								})
								break
						}
					}

					// Add newline after heading and list item closing tags
					if (/^(h[1-6]|li)$/.test(info.tag)) {
						text += '\n'
					}

					// Add newline after </p>
					if (info.tag === 'p') {
						text += '\n'
					}
				}

				continue
			}

			// Opening tag (not self-closing)
			if (!info.selfClosing) {
				stack.push({
					tag: info.tag,
					attrs: info.attrs,
					textStart: text.length,
				})
			}
		} else {
			// Text node
			if (stripDepth > 0 || bbImgCtnDepth > 0) continue
			text += decodeHtmlEntities(token)
		}
	}

	// Post-processing: collapse 3+ consecutive newlines to 2, then trim.
	// We need to adjust facet byte offsets to match the processed text.
	const rawText = text
	text = text.replace(/\n{3,}/g, '\n\n').trim()

	// Rebuild facets with correct byte offsets against the processed text.
	// We build a char-index mapping from raw → processed.
	const processedFacets: RichtextFacet[] = []

	// Find leading whitespace that was trimmed
	const leadingTrimmed = rawText.length - rawText.trimStart().length

	// Build the collapsed text from rawText to figure out char shifts
	const collapsed = rawText.replace(/\n{3,}/g, '\n\n')
	const trimStart = collapsed.length - collapsed.trimStart().length

	// Build a mapping from raw char index to collapsed char index
	// by walking through raw text and tracking where we are in collapsed
	const rawToCollapsed: number[] = new Array(rawText.length + 1)
	let rawIdx = 0
	let colIdx = 0
	const rawChars = rawText
	const colChars = collapsed

	while (rawIdx < rawChars.length && colIdx < colChars.length) {
		rawToCollapsed[rawIdx] = colIdx
		// If both chars match, advance both
		if (rawChars[rawIdx] === colChars[colIdx]) {
			rawIdx++
			colIdx++
		} else {
			// raw has extra newlines that were collapsed — skip raw char
			rawIdx++
		}
	}
	// Fill remaining (trailing chars that were consumed)
	while (rawIdx <= rawChars.length) {
		rawToCollapsed[rawIdx] = colIdx
		rawIdx++
	}

	for (const facet of facets) {
		// Convert raw byte offsets back to raw char positions
		// Since we built text by appending, the byte offsets correspond to raw text
		// We need raw char start/end to map through
		const rawCharStart = rawByteToChar(rawText, facet.index.byteStart)
		const rawCharEnd = rawByteToChar(rawText, facet.index.byteEnd)

		// Map to collapsed char positions
		const colCharStart = rawToCollapsed[rawCharStart] ?? 0
		const colCharEnd = rawToCollapsed[rawCharEnd] ?? collapsed.length

		// Apply trim offset
		const trimmedCharStart = colCharStart - trimStart
		const trimmedCharEnd = colCharEnd - trimStart

		// Skip facets that are entirely in trimmed region
		if (trimmedCharEnd <= 0 || trimmedCharStart >= text.length) continue

		// Clamp to valid range
		const clampedStart = Math.max(0, trimmedCharStart)
		const clampedEnd = Math.min(text.length, trimmedCharEnd)

		if (clampedEnd <= clampedStart) continue

		processedFacets.push({
			index: {
				byteStart: byteOffset(text, clampedStart),
				byteEnd: byteOffset(text, clampedEnd),
			},
			features: facet.features,
		})
	}

	return { text, facets: processedFacets }
}

/** Convert a UTF-8 byte offset to a char index in the given string. */
function rawByteToChar(str: string, targetBytes: number): number {
	let bytes = 0
	for (let i = 0; i < str.length; i++) {
		if (bytes >= targetBytes) return i
		bytes += byteLength(str[i]!)
	}
	return str.length
}
