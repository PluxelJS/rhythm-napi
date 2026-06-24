import { expect, test } from 'vitest'

import { Streamer } from '..'

test('ReplayGain recommendation is explicit and clipping-aware', () => {
  const streamer = new Streamer()

  const track = streamer.recommendReplayGain({
    mode: 'track',
    trackGainDb: -7.5,
    albumGainDb: -5,
    trackPeak: 0.5,
    albumPeak: 0.75,
    preampDb: 2,
  })

  expect(track.source).toBe('track')
  expect(track.gainDb).toBe(-5.5)
  expect(track.requestedGainDb).toBeCloseTo(-5.5, 6)
  expect(track.clippingLimited).toBe(false)
  expect(track.rangeLimited).toBe(false)

  const clipped = streamer.recommendReplayGain({
    mode: 'track',
    trackGainDb: 6,
    trackPeak: 0.9,
  })

  expect(clipped.source).toBe('track')
  expect(clipped.clippingLimited).toBe(true)
  expect(clipped.gainDb).toBeCloseTo(-0.08, 6)
  expect(clipped.requestedGainDb).toBeCloseTo(-0.08485, 4)

  const albumFallback = streamer.recommendReplayGain({
    mode: 'track',
    albumGainDb: -4,
    albumPeak: 0.8,
  })

  expect(albumFallback.source).toBe('album')
  expect(albumFallback.gainDb).toBe(-4)
})

test('ReplayGain recommendation rejects missing metadata without fallback', () => {
  const streamer = new Streamer()

  expect(() => streamer.recommendReplayGain({
    mode: 'track',
    albumGainDb: -4,
    fallbackToOther: false,
  })).toThrow(/ReplayGain metadata/)
})
