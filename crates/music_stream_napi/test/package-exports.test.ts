import { createRequire } from 'node:module'
import { expect, test } from 'vitest'

test('loads the same CommonJS runtime through import and require conditions', async () => {
	const imported = await import('@rhythm-app/streamer')
	const required = createRequire(__filename)('@rhythm-app/streamer') as typeof imported

	expect(imported.Streamer).toBe(required.Streamer)
})
