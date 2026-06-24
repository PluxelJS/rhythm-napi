import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from 'vitest'

import { Streamer } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  createHttpServerWorker,
  delay,
  isRtpForSsrc,
  makeSineWave,
  rtpTransport,
  stopStreamIfPresent,
  waitForDatagram,
  waitForStatus,
} from './helpers'

test('startStream next preload promotes to current and keeps sending RTP', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const currentPath = path.join(tempDir, 'current-short.wav')
  const nextPath = path.join(tempDir, 'next-promoted.wav')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `next-promote-${Date.now()}`
  const ssrc = 0x6d6e6f70

  await fs.promises.writeFile(currentPath, makeSineWave(0.25))
  await fs.promises.writeFile(nextPath, makeSineWave(1.2))

  try {
    const firstPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = streamer.startStream({
      streamId,
      current: {
        id: 'current-short',
        kind: 'file',
        path: currentPath,
        seekable: true,
      },
      next: {
        id: 'next-promoted',
        kind: 'file',
        path: nextPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    expect(started.current).toBeDefined()
    expect(started.current!.id).toBe('current-short')
    expect(started.next?.id).toBe('next-promoted')
    await firstPacket

    const promoted = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'next-promoted' && status.playState === 'playing',
      2_000,
    )
    expect(promoted.current?.id).toBe('next-promoted')

    const promotedPacket = await waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    expect(promotedPacket.message.readUInt32BE(8)).toBe(ssrc)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})

test('runtime controls work through the N-API boundary', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'controls.wav')
  const switchedWavPath = path.join(tempDir, 'controls-switched.wav')
  const liveServer = await createHttpServerWorker(makeSineWave(0.9))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `controls-${Date.now()}`
  const ssrc = 0x33343536

  await fs.promises.writeFile(wavPath, makeSineWave(2.2))
  await fs.promises.writeFile(switchedWavPath, makeSineWave(0.8))

  try {
    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    streamer.startStream({
      streamId,
      current: {
        id: 'controls',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
      volume: 1,
    })
    await receiver

    const volumeStatus = streamer.setVolume(streamId, 0.25)
    expect(volumeStatus.volume).toBeCloseTo(0.25, 6)
    const gainStatus = streamer.setGain(streamId, -3)
    expect(gainStatus.gainDb).toBe(-3)

    const paused = streamer.pauseStream(streamId)
    expect(paused.playState).toBe('paused')
    await delay(80)
    const stillPaused = streamer.getStatus(streamId)
    expect(stillPaused.playState).toBe('paused')
    expect(stillPaused.timePlayedMs).toBeLessThanOrEqual(paused.timePlayedMs + 20)

    streamer.resumeStream(streamId)
    const resumed = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing' && status.timePlayedMs > stillPaused.timePlayedMs,
    )
    expect(resumed.playState).toBe('playing')

    const seeked = streamer.seekStream(streamId, 1)
    expect(seeked.timePlayedMs).toBeGreaterThanOrEqual(1_000)

    const switchedReceiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const switched = streamer.switchTrack(streamId, {
      id: 'controls-switched',
      kind: 'file',
      path: switchedWavPath,
      seekable: true,
    }, null)
    expect(switched.current).toBeDefined()
    expect(switched.current!.id).toBe('controls-switched')
    await switchedReceiver
    const switchedPlaying = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'controls-switched' && status.playState === 'playing',
    )
    expect(switchedPlaying.current?.id).toBe('controls-switched')

    const liveReceiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const liveSwitched = streamer.switchTrack(streamId, {
      id: 'controls-live',
      kind: 'live',
      url: liveServer.url,
      seekable: true,
    }, null)
    expect(liveSwitched.current).toBeDefined()
    expect(liveSwitched.current!.id).toBe('controls-live')
    expect(liveSwitched.current!.kind).toBe('live')
    expect(liveSwitched.current!.seekable).toBe(false)
    expect(() => streamer.seekStream(streamId, 1)).toThrow(/not seekable/)
    await liveReceiver
    const livePlaying = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'controls-live' && status.playState === 'playing',
    )
    expect(livePlaying.current?.kind).toBe('live')
    expect(livePlaying.current?.seekable).toBe(false)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    await liveServer.close()
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})

test('stopStream preserves stopped status and reusable ids', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'stop-reuse.wav')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `stop-reuse-${Date.now()}`
  const ssrc = 0x41424344

  await fs.promises.writeFile(wavPath, makeSineWave(0.7))

  try {
    const firstPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    streamer.startStream({
      streamId,
      current: {
        id: 'stop-reuse',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    await firstPacket

    const stopped = streamer.stopStream(streamId)
    expect(stopped.playState).toBe('stopped')
    expect(streamer.getStatus(streamId).playState).toBe('stopped')
    expect(streamer.stopStream(streamId).playState).toBe('stopped')

    const secondSsrc = ssrc + 1
    const secondPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, secondSsrc))
    const restarted = streamer.startStream({
      streamId,
      current: {
        id: 'stop-reuse',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, secondSsrc),
    })
    expect(restarted.streamId).toBe(streamId)
    await secondPacket
  } finally {
    streamer.shutdown()
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})

test('shutdown stops active runtimes and clears stream registry', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'shutdown.wav')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `shutdown-${Date.now()}`
  const ssrc = 0x61626364

  await fs.promises.writeFile(wavPath, makeSineWave(1))

  try {
    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    streamer.startStream({
      streamId,
      current: {
        id: 'shutdown',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    await receiver

    streamer.shutdown()
    try {
      streamer.getStatus(streamId)
      throw new Error('expected getStatus to fail after shutdown')
    } catch (error) {
      expect((error as { code?: string }).code).toBe('StreamNotFound')
      expect((error as Error).message).toMatch(/stream not found/)
    }

    const restarted = streamer.startStream({
      streamId,
      current: {
        id: 'shutdown',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc + 1),
    })
    expect(restarted.streamId).toBe(streamId)
  } finally {
    streamer.shutdown()
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})

test('batch statuses return per-stream results and transport bitrate defaults to music quality', async () => {
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const activeStreamId = `batch-active-${Date.now()}`
  const stoppedStreamId = `batch-stopped-${Date.now()}`

  try {
    const defaultTransport = streamer.validateRtpTransportConfig(
      rtpTransport(socket, 0x51525354),
    )
    expect(defaultTransport.opusBitrateBps).toBe(128_000)

    const customTransport = streamer.validateRtpTransportConfig(
      rtpTransport(socket, 0x55565758, { bitrate: 192_000 }),
    )
    expect(customTransport.opusBitrateBps).toBe(192_000)

    streamer.startPlaceholderStream(activeStreamId, {
      id: 'batch-active-track',
      kind: 'file',
      path: '/tmp/batch-active.wav',
      seekable: true,
    }, null)
    streamer.startPlaceholderStream(stoppedStreamId, {
      id: 'batch-stopped-track',
      kind: 'file',
      path: '/tmp/batch-stopped.wav',
      seekable: true,
    }, null)
    streamer.stopStream(stoppedStreamId)

    const explicit = streamer.getStatuses([
      activeStreamId,
      stoppedStreamId,
      'missing-stream',
    ])
    expect(explicit).toHaveLength(3)
    expect(explicit[0]).toMatchObject({ streamId: activeStreamId, ok: true })
    expect(explicit[0].status?.current?.id).toBe('batch-active-track')
    expect(explicit[1]).toMatchObject({ streamId: stoppedStreamId, ok: true })
    expect(explicit[1].status?.playState).toBe('stopped')
    expect(explicit[2]).toMatchObject({
      streamId: 'missing-stream',
      ok: false,
      code: 'StreamNotFound',
    })
    expect(explicit[2].message).toMatch(/stream not found/)

    const all = streamer.getStatuses()
    expect(all.map((item) => item.streamId)).toEqual([
      activeStreamId,
      stoppedStreamId,
    ].sort())
  } finally {
    streamer.shutdown()
    await closeSocket(socket)
  }
})
