import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from 'vitest'

import { Streamer } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  createHttpServerWorker,
  createHttpStatusServerWorker,
  isRtpForSsrc,
  makeSineWave,
  rtpTransport,
  stopStreamIfPresent,
  waitForDatagram,
} from './helpers'

test('bounded HTTP URL source can stream and reuse a cached artifact', async () => {
  const body = makeSineWave(1)
  const server = await createHttpServerWorker(body)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `url-${Date.now()}`
  const cachedStreamId = `url-cached-${Date.now()}`
  const ssrc = 0x53545556
  const cachedSsrc = 0x57585960

  try {
    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = streamer.startStream({
      streamId,
      current: {
        id: 'url-tone',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
      volume: 1,
    })

    expect(started.current).toBeDefined()
    expect(started.current!.id).toBe('url-tone')
    expect(started.current!.kind).toBe('url')
    expect((await receiver).message.readUInt32BE(8)).toBe(ssrc)

    streamer.stopStream(streamId)
    await server.close()

    const cachedReceiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, cachedSsrc))
    streamer.startStream({
      streamId: cachedStreamId,
      current: {
        id: 'url-tone',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, cachedSsrc),
      volume: 1,
    })
    expect((await cachedReceiver).message.readUInt32BE(8)).toBe(cachedSsrc)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    stopStreamIfPresent(streamer, cachedStreamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('source resolver policy validates limits and uncached restart behavior', async () => {
  const body = makeSineWave(1)
  const server = await createHttpServerWorker(body)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `source-policy-${Date.now()}`
  const uncachedStreamId = `source-policy-uncached-${Date.now()}`
  const ssrc = 0x63646566
  const uncachedSsrc = 0x67686970

  try {
    const normalized = streamer.validateSourceResolverConfig({
      http: {
        timeoutMs: 500,
        maxBytes: body.length + 1024,
        cacheTempFiles: false,
      },
      liveHttp: {
        timeoutMs: 700,
        maxBufferedBytes: 32_768,
        readChunkBytes: 4_096,
        maxRetries: 1,
        retryBackoffMs: 20,
      },
    })
    expect(normalized.http.timeoutMs).toBe(500)
    expect(normalized.http.maxBytes).toBe(body.length + 1024)
    expect(normalized.http.cacheTempFiles).toBe(false)
    expect(normalized.liveHttp.timeoutMs).toBe(700)
    expect(normalized.liveHttp.maxBufferedBytes).toBe(32_768)
    expect(normalized.liveHttp.readChunkBytes).toBe(4_096)
    expect(normalized.liveHttp.maxRetries).toBe(1)
    expect(normalized.liveHttp.retryBackoffMs).toBe(20)

    expect(() => streamer.startStream({
      streamId,
      current: {
        id: 'source-policy-too-large',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
      source: {
        http: {
          maxBytes: 128,
          cacheTempFiles: false,
        },
      },
    })).toThrow(/exceeds max size/)

    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, uncachedSsrc))
    streamer.startStream({
      streamId: uncachedStreamId,
      current: {
        id: 'source-policy-uncached',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, uncachedSsrc),
      source: {
        http: {
          timeoutMs: 500,
          maxBytes: body.length + 1024,
          cacheTempFiles: false,
        },
      },
    })
    await receiver
    streamer.stopStream(uncachedStreamId)
    await server.close()

    expect(() => streamer.startStream({
      streamId: `${uncachedStreamId}-restart`,
      current: {
        id: 'source-policy-uncached',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, uncachedSsrc + 1),
      source: {
        http: {
          timeoutMs: 500,
          maxBytes: body.length + 1024,
          cacheTempFiles: false,
        },
      },
    })).toThrow(/error sending request|Connection refused|connection refused|tcp connect/)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    stopStreamIfPresent(streamer, uncachedStreamId)
    stopStreamIfPresent(streamer, `${uncachedStreamId}-restart`)
    await server.close()
    await closeSocket(socket)
  }
})

test('auth-expired URL source requests source refresh through events', async () => {
  const server = await createHttpStatusServerWorker(403)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `auth-expired-${Date.now()}`

  try {
    expect(() => streamer.startStream({
      streamId,
      current: {
        id: 'auth-track',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, 0x797a7b7c),
      source: {
        http: {
          timeoutMs: 500,
          maxBytes: 1024 * 1024,
        },
      },
    })).toThrow(/source auth expired|403/)

    const events = streamer.drainEvents(streamId)
    expect(events.some((event) => (
      event.type === 'sourceRefreshNeeded' &&
      event.streamId === streamId &&
      event.trackId === 'auth-track'
    ))).toBe(true)
  } finally {
    streamer.shutdown()
    await server.close()
    await closeSocket(socket)
  }
})

test('auth-expired next preload does not abort current playback', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'current.wav')
  const server = await createHttpStatusServerWorker(403)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `next-auth-expired-${Date.now()}`
  const ssrc = 0x7d7e7f80

  await fs.promises.writeFile(wavPath, makeSineWave(0.5))

  try {
    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = streamer.startStream({
      streamId,
      current: {
        id: 'current-ok',
        kind: 'file',
        path: wavPath,
        seekable: true,
      },
      next: {
        id: 'next-expired',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
      source: {
        http: {
          timeoutMs: 500,
          maxBytes: 1024 * 1024,
        },
      },
    })
    expect(started.current).toBeDefined()
    expect(started.current!.id).toBe('current-ok')
    await receiver

    const events = streamer.drainEvents(streamId)
    expect(events.some((event) => (
      event.type === 'sourceRefreshNeeded' &&
      event.streamId === streamId &&
      event.trackId === 'next-expired'
    ))).toBe(true)
  } finally {
    streamer.shutdown()
    await server.close()
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})
