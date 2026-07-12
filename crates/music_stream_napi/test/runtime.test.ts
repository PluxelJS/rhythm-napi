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
