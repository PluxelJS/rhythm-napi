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
  waitForStatus,
} from './helpers'

test('live current streams over RTP and stays non-seekable', async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-stream-napi-'))
  const wavPath = path.join(tempDir, 'non-seekable.wav')
  const liveServer = await createHttpServerWorker(makeSineWave(0.7))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const liveStreamId = `live-${Date.now()}`
  const fileStreamId = `non-seekable-${Date.now()}`
  const liveSsrc = 0x71727374
  const fileSsrc = 0x75767778

  await fs.promises.writeFile(wavPath, makeSineWave(1))

  try {
    const liveReceiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, liveSsrc))
    const liveStarted = streamer.startStream({
      streamId: liveStreamId,
      current: {
        id: 'live',
        kind: 'live',
        url: liveServer.url,
        seekable: true,
      },
      transport: rtpTransport(socket, liveSsrc),
    })
    expect(liveStarted.current).toBeDefined()
    expect(liveStarted.current!.id).toBe('live')
    expect(liveStarted.current!.kind).toBe('live')
    expect(liveStarted.current!.seekable).toBe(false)
    expect((await liveReceiver).message.readUInt32BE(8)).toBe(liveSsrc)
    expect(() => streamer.seekStream(liveStreamId, 1)).toThrow(/not seekable/)
    streamer.stopStream(liveStreamId)

    const receiver = waitForDatagram(socket, (message) => isRtpForSsrc(message, fileSsrc))
    const fileStarted = streamer.startStream({
      streamId: fileStreamId,
      current: {
        id: 'non-seekable-file',
        kind: 'file',
        path: wavPath,
        seekable: false,
      },
      transport: rtpTransport(socket, fileSsrc),
    })
    expect(fileStarted.next).toBeUndefined()
    await receiver
    const setNextStatus = streamer.setNext(fileStreamId, {
      id: 'live-next',
      kind: 'live',
      url: liveServer.url,
    })
    expect(setNextStatus.next).toBeUndefined()
    const liveNextEvents = streamer.drainEvents(fileStreamId)
    expect(liveNextEvents.some((event) => (
      event.type === 'error' &&
      event.streamId === fileStreamId &&
      event.code === 'Unsupported' &&
      typeof event.message === 'string' &&
      /live sources cannot be preloaded/.test(event.message)
    ))).toBe(true)
    expect(streamer.getStatus(fileStreamId).next).toBeUndefined()
    expect(() => streamer.seekStream(fileStreamId, 1)).toThrow(/not seekable/)
  } finally {
    stopStreamIfPresent(streamer, liveStreamId)
    stopStreamIfPresent(streamer, fileStreamId)
    await liveServer.close()
    await closeSocket(socket)
    await fs.promises.rm(tempDir, { recursive: true, force: true })
  }
})

test('live startup source failure after retry rejects without leaking stream', async () => {
  const server = await createHttpStatusServerWorker(503)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `live-fail-${Date.now()}`

  try {
    expect(() => streamer.startStream({
      streamId,
      current: {
        id: 'live-fail',
        kind: 'live',
        url: server.url,
      },
      transport: rtpTransport(socket, 0x81828384),
      source: {
        liveHttp: {
          timeoutMs: 500,
        },
      },
    })).toThrow(/invalid source|503/)
    expect(() => streamer.getStatus(streamId)).toThrow(/stream not found/)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('live startup auth failure requests source refresh without leaking stream', async () => {
  const server = await createHttpStatusServerWorker(403)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `live-auth-${Date.now()}`

  try {
    expect(() => streamer.startStream({
      streamId,
      current: {
        id: 'live-auth',
        kind: 'live',
        url: server.url,
      },
      transport: rtpTransport(socket, 0x85868788),
      source: {
        liveHttp: {
          timeoutMs: 500,
        },
      },
    })).toThrow(/source auth expired|403/)

    const events = streamer.drainEvents(streamId)
    expect(events.some((event) => (
      event.type === 'sourceRefreshNeeded' &&
      event.streamId === streamId &&
      event.trackId === 'live-auth'
    ))).toBe(true)
    expect(() => streamer.getStatus(streamId)).toThrow(/stream not found/)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('live current can be refreshed on the same stream after source exhaustion', async () => {
  const firstServer = await createHttpServerWorker(makeSineWave(0.12))
  const refreshedServer = await createHttpServerWorker(makeSineWave(0.3))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `live-refresh-${Date.now()}`
  const ssrc = 0x91929394

  try {
    const firstPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    streamer.startStream({
      streamId,
      current: {
        id: 'live-refresh',
        kind: 'live',
        url: firstServer.url,
      },
      transport: rtpTransport(socket, ssrc),
      source: {
        liveHttp: {
          timeoutMs: 500,
        },
      },
    })
    await firstPacket
    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'idle' && status.current === undefined,
      2_000,
    )

    const refreshedPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const refreshed = streamer.refreshCurrentSource(streamId, {
      id: 'live-refresh',
      kind: 'live',
      url: refreshedServer.url,
    })

    expect(refreshed.current).toBeDefined()
    expect(refreshed.current!.url).toBe(refreshedServer.url)
    expect(refreshed.current!.seekable).toBe(false)
    expect((await refreshedPacket).message.readUInt32BE(8)).toBe(ssrc)
  } finally {
    stopStreamIfPresent(streamer, streamId)
    await firstServer.close()
    await refreshedServer.close()
    await closeSocket(socket)
  }
})
