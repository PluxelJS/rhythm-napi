import { expect, test } from 'vitest'

import { Streamer, type StreamEventOutput } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  createHttpStatusServerWorker,
  rtpTransport,
} from './helpers'

test('source and transport policies reject invalid limits synchronously', () => {
  const streamer = new Streamer()
  expect(() => new Streamer({
    maxBlockingProducers: 4,
    maxBlockingPreloads: 4,
  })).toThrow(/resource|limit|invalid/i)
  expect(() => new Streamer({
    maxConcurrentLiveStreams: 1,
  })).toThrow(/resource|limit|invalid/i)
  expect(() => streamer.validateSourceResolverConfig({
    http: { ioTimeoutMs: 0 },
  })).toThrow(/greater than zero/)
  expect(() => streamer.validateSourceResolverConfig({
    liveHttp: { retryBackoffMs: 0 },
  })).toThrow(/non-zero/)
  expect(() => streamer.validateSourceResolverConfig({
    liveHttp: { idleTimeoutMs: 0 },
  })).toThrow(/greater than zero/)
  expect(() => streamer.validateRtpTransportConfig({
    ip: '127.0.0.1',
    port: 0,
    audioSsrc: 1,
  })).toThrow(/port/)
})

test('HTTP authorization failure is reported asynchronously', async () => {
  const server = await createHttpStatusServerWorker(403)
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer()
  const streamId = `auth-${Date.now()}`

  try {
    await streamer.startStream({
      streamId,
      current: { id: 'auth', kind: 'url', url: server.url, seekable: true },
      transport: rtpTransport(socket, 0x44556677),
    })
    const events: StreamEventOutput[] = []
    const deadline = Date.now() + 2_000
    while (Date.now() < deadline && !events.some((event) => event.code === 'SOURCE_AUTH_EXPIRED')) {
      await new Promise((resolve) => setTimeout(resolve, 20))
      events.push(...streamer.drainEvents(streamId))
    }
    expect(
      events.some((event) => event.type === 'sourceRefreshNeeded'),
      JSON.stringify(events),
    ).toBe(true)
    expect(
      events.some((event) => event.code === 'SOURCE_AUTH_EXPIRED'),
      JSON.stringify(events),
    ).toBe(true)
  } finally {
    await streamer.shutdown()
    await server.close()
    await closeSocket(socket)
  }
})
