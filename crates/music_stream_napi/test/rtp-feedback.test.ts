import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from 'vitest'

import { Streamer, type StreamEventOutput } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  isRtpForSsrc,
  makeReceiverReport,
  makeSineWave,
  rtpTransport,
  send,
  stopStreamIfPresent,
  waitForDatagram,
  waitForEvent,
  waitForStatus,
} from './helpers'

test('local file RTP exposes RTCP receiver feedback and quality events', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'tone.wav')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `smoke-${Date.now()}`
  const ssrc = 0x23242526
  const callbackEvents: StreamEventOutput[] = []

  await fs.promises.writeFile(wavPath, makeSineWave())
  streamer.setEventCallback((event) => {
    callbackEvents.push(event)
  })

  try {
    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = streamer.startStream({
      streamId,
      current: {
        id: 'tone',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc, { encryption: { mode: 'none' } }),
      volume: 0.65,
      gainDb: 1.5,
    })

    expect(started.streamId).toBe(streamId)
    expect(started.current).toBeDefined()
    expect(started.current!.id).toBe('tone')
    expect(started.volume).toBeCloseTo(0.65, 6)
    expect(started.gainDb).toBe(1.5)

    const startupEvents = streamer.drainEvents(streamId)
    expect(startupEvents.some((event) => (
      event.type === 'stateChanged' &&
      event.status?.playState === 'idle' &&
      Math.abs(event.status.volume - 0.65) < 0.000001 &&
      event.status.gainDb === 0
    ))).toBe(true)
    expect(startupEvents.some((event) => (
      event.type === 'stateChanged' &&
      event.status?.playState === 'idle' &&
      Math.abs(event.status.volume - 0.65) < 0.000001 &&
      event.status.gainDb === 1.5
    ))).toBe(true)

    const callbackPlaying = await waitForEvent(
      callbackEvents,
      (event) => event.type === 'stateChanged' && event.streamId === streamId && event.status?.playState === 'playing',
    )
    expect(callbackPlaying.streamId).toBe(streamId)

    const drained = [...startupEvents, ...streamer.drainEvents(streamId)]
    expect(drained.some((event) => event.type === 'stateChanged' && event.streamId === streamId)).toBe(true)
    for (const event of drained.filter((event) => event.type === 'stateChanged' && event.streamId === streamId)) {
      expect(event.status).toBeDefined()
      expect(event.status!.volume).toBeCloseTo(0.65, 6)
      if (event.status!.playState !== 'idle' || event.status!.gainDb !== 0) {
        expect(event.status!.gainDb).toBe(1.5)
      }
    }

    const { message, rinfo } = await receiver
    expect(message.readUInt32BE(8)).toBe(ssrc)

    await send(socket, makeReceiverReport({ sourceSsrc: ssrc, fractionLost: 13, totalLost: 2, jitter: 123 }), rinfo.port, rinfo.address)

    const status = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => Boolean(status.receiverReport),
      2_000,
    )
    const report = status.receiverReport

    expect(report).toBeDefined()
    expect(report!.sourceSsrc).toBe(ssrc)
    expect(report!.fractionLost).toBe(13)
    expect(report!.totalLost).toBe(2)
    expect(report!.jitter).toBe(123)
    expect(report!.jitterMicros).toBe(2_562)
    expect(report!.jitterMs).toBe(2.562)
    expect(report!.roundTripTimeMs).toBeUndefined()

    const qualityEvent = await waitForEvent(
      callbackEvents,
      (event) => event.type === 'networkQualityChanged' && event.streamId === streamId,
    )
    expect(qualityEvent.quality).toBe('degraded')
    expect(qualityEvent.qualitySamples).toBe(1)
    expect(qualityEvent.latestLossPercent).toBeCloseTo(5.078125, 6)
    expect(qualityEvent.averageLossPercent).toBeCloseTo(5.078125, 6)
    expect(qualityEvent.maxLossPercent).toBeCloseTo(5.078125, 6)
    expect(qualityEvent.averageJitterMs).toBeCloseTo(2.562, 6)
    expect(qualityEvent.maxJitterMs).toBeCloseTo(2.562, 6)
    expect(qualityEvent.averageRoundTripTimeMs).toBeUndefined()
    expect(qualityEvent.maxRoundTripTimeMs).toBeUndefined()
  } finally {
    stopStreamIfPresent(streamer, streamId)
    streamer.setEventCallback(null)
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})
