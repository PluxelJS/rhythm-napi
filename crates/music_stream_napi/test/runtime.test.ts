import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from 'vitest'

import { Streamer } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  createHlsServerWorker,
  createHttpServerWorker,
  delay,
  isRtpForSsrc,
  makeSineWave,
  rtpTransport,
  stopStreamIfPresent,
  waitForDatagram,
  waitForStatus,
} from './helpers'

test('status queries enforce bounded validated stream identifiers', async () => {
  const streamer = new Streamer()
  try {
    await expect(streamer.getStatus(' '.repeat(8))).rejects.toThrow(/INVALID_CONFIG.*stream id/i)
    await expect(streamer.getStatuses(
      Array.from({ length: 4_097 }, (_, index) => `stream-${index}`),
    )).rejects.toThrow(/INVALID_CONFIG.*4096/i)
    expect(() => streamer.drainEvents('x'.repeat(513))).toThrow(/INVALID_CONFIG.*stream id/i)
  } finally {
    await streamer.shutdown()
  }
})

test('all lifecycle methods are asynchronous and RTP remains monotonic across switch', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-runtime-'))
  const firstPath = path.join(directory, 'first.wav')
  const secondPath = path.join(directory, 'second.wav')
  await fs.promises.writeFile(firstPath, makeSineWave(1))
  await fs.promises.writeFile(secondPath, makeSineWave(1))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer({ maxStreams: 1 })
  const streamId = `runtime-${Date.now()}`
  const ssrc = 0x11223344

  try {
    const firstPacket = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = await streamer.startStream({
      streamId,
      current: { id: 'first', kind: 'file', path: firstPath, seekable: true },
      transport: rtpTransport(socket, ssrc),
    })
    expect(started.current?.id).toBe('first')
    expect(started.current && 'path' in started.current).toBe(false)
    const before = (await firstPacket).message
    const activeStatus = await streamer.getStatus(streamId)
    expect(activeStatus.playoutDiagnostics?.packetsSent).toBeGreaterThan(0)
    expect(activeStatus.playoutDiagnostics?.bytesSent).toBeGreaterThan(0)
    expect(activeStatus.playoutDiagnostics?.bufferedMs).toBeGreaterThanOrEqual(0)
    expect(activeStatus.playoutDiagnostics?.underruns).toBeGreaterThanOrEqual(0)
    await expect(streamer.startStream({
      streamId: `${streamId}-overflow`,
      current: { id: 'overflow', kind: 'file', path: firstPath },
      transport: rtpTransport(socket, ssrc + 1),
    })).rejects.toThrow(/BUSY.*stream limit/i)

    const paused = await streamer.pauseStream(streamId)
    expect(paused.playState).toBe('paused')
    await delay(60)
    const pausedStatus = await streamer.getStatus(streamId)
    expect(pausedStatus.playState).toBe('paused')
    const batch = await streamer.getStatuses([streamId, `${streamId}-missing`])
    expect(batch[0].ok).toBe(true)
    expect(batch[1].code).toBe('STREAM_NOT_FOUND')
    await streamer.resumeStream(streamId)

    const switched = await streamer.switchTrack(
      streamId,
      { id: 'second', kind: 'file', path: secondPath, seekable: true },
      null,
    )
    expect(switched.current?.id).toBe('second')
    const after = (await waitForDatagram(
      socket,
      (message) => isRtpForSsrc(message, ssrc) && (message[1] & 0x80) !== 0,
    )).message
    expect(after.readUInt16BE(2)).toBeGreaterThan(before.readUInt16BE(2))
    expect(after.readUInt32BE(4)).toBeGreaterThan(before.readUInt32BE(4))

    const seeked = await streamer.seekStream(streamId, 0)
    expect(seeked.current?.id).toBe('second')
    const stopped = await streamer.stopStream(streamId)
    expect(stopped.playState).toBe('stopped')
    expect((await streamer.getStatus(streamId)).playState).toBe('stopped')

    const replacementId = `${streamId}-replacement`
    await streamer.startStream({
      streamId: replacementId,
      current: { id: 'replacement', kind: 'file', path: secondPath },
      transport: rtpTransport(socket, ssrc + 2),
    })
    await streamer.stopStream(replacementId)
    await expect(streamer.getStatus(streamId)).rejects.toThrow(/STREAM_NOT_FOUND/)
    await streamer.shutdown()
    await streamer.shutdown()
    await expect(streamer.startStream({
      streamId: `${streamId}-after-shutdown`,
      current: { id: 'closed', kind: 'file', path: firstPath },
      transport: rtpTransport(socket, ssrc + 3),
    })).rejects.toThrow(/STREAM_CLOSED.*shut down/i)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('seek near the end preserves repeated automatic preload promotion', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-seek-promotion-'))
  const paths = await Promise.all(
    [2.2, 1, 1].map(async (seconds, index) => {
      const audioPath = path.join(directory, `${index}.wav`)
      await fs.promises.writeFile(audioPath, makeSineWave(seconds))
      return audioPath
    }),
  )
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `seek-promotion-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'a', kind: 'file', path: paths[0] },
      next: { id: 'b', kind: 'file', path: paths[1] },
      transport: rtpTransport(socket, 0x11223345),
    })
    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing' && status.current?.id === 'a',
    )
    await streamer.seekStream(streamId, 2)

    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'b' && status.playState === 'playing',
    ).catch(async (error: unknown) => {
      throw new Error(`b did not promote after seek: ${JSON.stringify(await streamer.getStatus(streamId))}`, {
        cause: error,
      })
    })
    await streamer.setNext(
      streamId,
      { id: 'c', kind: 'file', path: paths[2] },
    )
    const final = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'c' && status.playState === 'playing',
    ).catch(async (error: unknown) => {
      throw new Error(`c did not promote after b: ${JSON.stringify(await streamer.getStatus(streamId))}`, {
        cause: error,
      })
    })
    expect(final.current?.id).toBe('c')
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('immediate seek to the end releases promoted HTTP preload quota for the following track', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-immediate-seek-'))
  const currentPath = path.join(directory, 'current.wav')
  await fs.promises.writeFile(currentPath, makeSineWave(2.2))
  const delayedNext = makeSineWave(0.8)
  const server = http.createServer((_request, response) => {
    setTimeout(() => {
      response.writeHead(200, {
        'content-length': delayedNext.length,
        'content-type': 'audio/wav',
      })
      response.end(delayedNext)
    }, 350)
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('HTTP test server did not bind')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `immediate-seek-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'a', kind: 'file', path: currentPath },
      next: {
        id: 'b',
        kind: 'url',
        url: `http://127.0.0.1:${address.port}/b.wav`,
        formatHint: 'wav',
      },
      transport: rtpTransport(socket, 0x11223346),
      source: { http: { maxRetries: 0 } },
    })
    await streamer.seekStream(streamId, 2)

    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'b' && status.playState === 'playing',
    ).catch(async (error: unknown) => {
      throw new Error(`delayed b did not promote: ${JSON.stringify(await streamer.getStatus(streamId))}`, {
        cause: error,
      })
    })
    await streamer.setNext(
      streamId,
      {
        id: 'c',
        kind: 'url',
        url: `http://127.0.0.1:${address.port}/c.wav`,
        formatHint: 'wav',
      },
    )
    const following = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'c' && status.playState === 'playing',
    )
    expect(following.current?.id).toBe('c')
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('bounded URL startup does not block the JavaScript event loop', async () => {
  const server = await createHttpServerWorker(makeSineWave(0.5))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `url-${Date.now()}`
  const ssrc = 0x22334455
  let heartbeat = false

  try {
    setTimeout(() => { heartbeat = true }, 0)
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = await streamer.startStream({
      streamId,
      current: {
        id: 'url',
        kind: 'url',
        url: server.url,
        formatHint: 'WAV',
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    await delay(0)
    expect(heartbeat).toBe(true)
    expect(started.current?.kind).toBe('url')
    expect(started.current?.formatHint).toBe('wav')
    expect(started.current && 'url' in started.current).toBe(false)
    await packet
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('failed next preload after current end exits buffering and requests a replacement', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-next-failure-'))
  const currentPath = path.join(directory, 'current.wav')
  const recoveredPath = path.join(directory, 'recovered.wav')
  const followingPath = path.join(directory, 'following.wav')
  await fs.promises.writeFile(currentPath, makeSineWave(0.04))
  await fs.promises.writeFile(recoveredPath, makeSineWave(0.5))
  await fs.promises.writeFile(followingPath, makeSineWave(0.5))
  const server = http.createServer((_request, response) => {
    setTimeout(() => {
      response.writeHead(503)
      response.end()
    }, 150)
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('HTTP test server did not bind')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `next-failure-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'current', kind: 'file', path: currentPath },
      next: {
        id: 'failed-next',
        kind: 'url',
        url: `http://127.0.0.1:${address.port}/next.wav`,
        formatHint: 'wav',
      },
      transport: rtpTransport(socket, 0x33445567),
      source: { http: { maxRetries: 0 } },
    })

    const idle = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'idle' && !status.current && !status.next,
    )
    expect(idle.playState).toBe('idle')
    const events = streamer.drainEvents(streamId)
    const state = events.find(
      (event) => event.type === 'stateChanged' && event.status?.playState === 'idle',
    )
    const request = events.find((event) => event.type === 'nextNeeded')
    expect(state).toBeDefined()
    expect(request).toBeDefined()
    expect(state!.sequence).toBeLessThan(request!.sequence)

    await streamer.switchTrack(
      streamId,
      { id: 'recovered', kind: 'file', path: recoveredPath },
      { id: 'following', kind: 'file', path: followingPath },
    )
    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'recovered' && status.playState === 'playing',
    )
    const following = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.current?.id === 'following' && status.playState === 'playing',
    )
    expect(following.current?.id).toBe('following')
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('a pending promoted preload is bounded by its attempt startup deadline', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-next-deadline-'))
  const currentPath = path.join(directory, 'current.wav')
  await fs.promises.writeFile(currentPath, makeSineWave(0.04))
  const server = http.createServer(() => {
    // Deliberately never publish response headers. The attempt supervisor, rather than the
    // longer source I/O timeout, must terminate this occurrence.
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('HTTP test server did not bind')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `next-deadline-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'current', attemptId: 'entry-a:attempt-1', kind: 'file', path: currentPath },
      next: {
        id: 'pending-next',
        attemptId: 'entry-b:attempt-2',
        kind: 'url',
        url: `http://127.0.0.1:${address.port}/next.wav`,
        formatHint: 'wav',
      },
      transport: rtpTransport(socket, 0x33445568),
      attemptStartTimeoutMs: 80,
      source: { http: { ioTimeoutMs: 5_000, maxRetries: 0 } },
    })

    await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'idle' && !status.current && !status.next,
    )
    const events = streamer.drainEvents(streamId)
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'attemptFailed',
        attemptId: 'entry-b:attempt-2',
        trackId: 'pending-next',
        sourceRole: 'next',
        code: 'SOURCE_TIMEOUT',
      }),
    )
    const state = events.find(
      (event) => event.type === 'stateChanged' && event.status?.playState === 'idle',
    )
    const request = events.find((event) => event.type === 'nextNeeded')
    expect(state).toBeDefined()
    expect(request).toBeDefined()
    expect(state!.sequence).toBeLessThan(request!.sequence)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
    await closeSocket(socket)
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('desired plans are versioned and promote the exact prepared attempt', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-plan-version-'))
  const firstPath = path.join(directory, 'first.wav')
  const secondPath = path.join(directory, 'second.wav')
  const thirdPath = path.join(directory, 'third.wav')
  const audio = makeSineWave(2)
  await Promise.all([
    fs.promises.writeFile(firstPath, audio),
    fs.promises.writeFile(secondPath, audio),
    fs.promises.writeFile(thirdPath, audio),
  ])
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `desired-plan-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'first', attemptId: 'attempt-a', kind: 'file', path: firstPath },
      next: { id: 'second', attemptId: 'attempt-b', kind: 'file', path: secondPath },
      transport: rtpTransport(socket, 0x33445569),
    })
    const before = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.next?.attemptId === 'attempt-b',
    )
    const preparedGeneration = before.next ? before.generation + 1 : -1

    const promoted = await streamer.reconcilePlan(streamId, {
      version: 1,
      current: { id: 'second', attemptId: 'attempt-b', kind: 'file', path: secondPath },
      next: { id: 'third', attemptId: 'attempt-c', kind: 'file', path: thirdPath },
    })
    expect(promoted).toMatchObject({
      current: { id: 'second', attemptId: 'attempt-b' },
      next: { id: 'third', attemptId: 'attempt-c' },
      planVersion: 1,
    })
    expect(promoted.generation).toBe(preparedGeneration)

    const stale = await streamer.reconcilePlan(streamId, {
      version: 1,
      current: { id: 'first', attemptId: 'stale-attempt', kind: 'file', path: firstPath },
    })
    expect(stale).toMatchObject({
      current: { id: 'second', attemptId: 'attempt-b' },
      next: { id: 'third', attemptId: 'attempt-c' },
      planVersion: 1,
    })
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await closeSocket(socket)
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})

test('live HTTP uses the same bounded producer and persistent sender', async () => {
  const server = await createHttpServerWorker(makeSineWave(0.6))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `live-${Date.now()}`
  const ssrc = 0x33445566

  try {
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = await streamer.startStream({
      streamId,
      current: { id: 'live', kind: 'live', url: server.url, seekable: true },
      transport: rtpTransport(socket, ssrc),
    })
    expect(started.current?.seekable).toBe(false)
    await packet
    const playing = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing',
    )
    expect(playing.current?.kind).toBe('live')
    await expect(streamer.seekStream(streamId, 1)).rejects.toThrow(/not seekable/)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('misclassified Icecast URL automatically uses live semantics', async () => {
  const server = await createHttpServerWorker(makeSineWave(0.6), {
    'icy-name': 'Rhythm Test Radio',
  })
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `detected-live-${Date.now()}`
  const ssrc = 0x33445567

  try {
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    await streamer.startStream({
      streamId,
      current: {
        id: 'misclassified-radio',
        kind: 'url',
        url: server.url,
        formatHint: 'wav',
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    await packet
    const playing = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing' && status.current?.kind === 'live',
    )
    expect(playing.current?.seekable).toBe(false)
    await expect(streamer.pauseStream(streamId)).rejects.toThrow(/live sources cannot be paused/)
    await expect(streamer.seekStream(streamId, 1)).rejects.toThrow(/not seekable/)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('HLS VOD remains a finite non-seekable streaming source', async () => {
  const server = await createHlsServerWorker(makeSineWave(0.6))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `hls-${Date.now()}`
  const ssrc = 0x33445568

  try {
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = await streamer.startStream({
      streamId,
      current: {
        id: 'hls-radio',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    expect(started.current?.kind).toBe('url')
    expect(started.current?.seekable).toBe(false)
    await packet
    const playing = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing',
    )
    expect(playing.current?.id).toBe('hls-radio')
    expect(playing.current?.kind).toBe('url')
    expect(playing.current?.seekable).toBe(false)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('opaque HLS VOD is detected without being mislabeled as live', async () => {
  const server = await createHlsServerWorker(makeSineWave(0.6), 'signed-manifest')
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `detected-hls-${Date.now()}`
  const ssrc = 0x33445569

  try {
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    await streamer.startStream({
      streamId,
      current: {
        id: 'opaque-hls-radio',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    await packet
    const playing = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing' && status.current?.kind === 'url',
    )
    expect(playing.current?.seekable).toBe(false)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('HLS without an end list is classified as live after probing the media playlist', async () => {
  const server = await createHlsServerWorker(makeSineWave(0.6), 'live.m3u8', true)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `live-hls-${Date.now()}`
  const ssrc = 0x33445570

  try {
    const packet = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
    const started = await streamer.startStream({
      streamId,
      current: {
        id: 'live-hls-radio',
        kind: 'url',
        url: server.url,
        seekable: true,
      },
      transport: rtpTransport(socket, ssrc),
    })
    expect(started.current?.kind).toBe('url')
    expect(started.current?.seekable).toBe(false)
    await packet
    const playing = await waitForStatus(
      () => streamer.getStatus(streamId),
      (status) => status.playState === 'playing' && status.current?.kind === 'live',
    )
    expect(playing.current?.seekable).toBe(false)
    await expect(streamer.pauseStream(streamId)).rejects.toThrow(/live sources cannot be paused/)
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await server.close()
    await closeSocket(socket)
  }
})

test('concurrent starts reserve ids independently and reject duplicate races', async () => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-concurrent-'))
  const audioPath = path.join(directory, 'audio.wav')
  await fs.promises.writeFile(audioPath, makeSineWave(0.5))
  const sockets = await Promise.all(Array.from({ length: 4 }, () => createBoundUdpSocket()))
  const streamer = new Streamer()
  const ids = sockets.map((_, index) => `concurrent-${Date.now()}-${index}`)

  try {
    const statuses = await Promise.all(sockets.map((socket, index) => streamer.startStream({
      streamId: ids[index],
      current: { id: `track-${index}`, kind: 'file', path: audioPath, seekable: true },
      transport: rtpTransport(socket, 0x55000000 + index),
    })))
    expect(statuses.map((status) => status.streamId)).toEqual(ids)
    expect((await streamer.getStatuses(ids)).every((item) => item.ok)).toBe(true)

    const duplicate = await Promise.allSettled([
      streamer.startStream({
        streamId: 'duplicate-race',
        current: { id: 'duplicate-a', kind: 'file', path: audioPath, seekable: true },
        transport: rtpTransport(sockets[0], 0x66000001),
      }),
      streamer.startStream({
        streamId: 'duplicate-race',
        current: { id: 'duplicate-b', kind: 'file', path: audioPath, seekable: true },
        transport: rtpTransport(sockets[1], 0x66000002),
      }),
    ])
    expect(duplicate.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
    expect(duplicate.filter((result) => result.status === 'rejected')).toHaveLength(1)
  } finally {
    await streamer.shutdown()
    await Promise.all(sockets.map(closeSocket))
    await fs.promises.rm(directory, { recursive: true, force: true })
  }
})
