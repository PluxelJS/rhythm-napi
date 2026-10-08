import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import test from 'node:test'

import { Streamer } from '@rhythm-app/streamer'

const require = createRequire(import.meta.url)

test('package import and require load the same native binding', () => {
  assert.equal(Streamer, require('@rhythm-app/streamer').Streamer)
})

test('native runtime resolves queries, rejects invalid input, and shuts down', async () => {
  const streamer = new Streamer()
  try {
    assert.deepEqual(await streamer.getStatuses([]), [])
    await assert.rejects(streamer.getStatus(' '), /INVALID_CONFIG/)
  } finally {
    await streamer.shutdown()
  }
})
