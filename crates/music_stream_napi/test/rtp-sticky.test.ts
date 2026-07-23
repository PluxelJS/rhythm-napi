import { expect, test } from 'vitest'

import { Streamer } from '..'
import {
  closeSocket,
  createBoundUdpSocket,
  createHttpServerWorker,
  makeSineWave,
  rtpTransport,
  stopStreamIfPresent,
  waitForStatus,
} from './helpers'

test('RTP keeps promoting constrained unique HTTP sources after repeated boundaries', async () => {
  const server = await createHttpServerWorker(makeSineWave(0.3))
  const socket = await createBoundUdpSocket()
  const streamer = new Streamer({
    maxBlockingPreloads: 1,
    maxConcurrentHttpDownloads: 2,
    maxTempfileBytes: 4 * 1024 * 1024,
  })
  const streamId = `rtp-http-sticky-${Date.now()}`
  const source = (index: number) => ({
    id: `http-${index}`,
    kind: 'url' as const,
    url: `${server.url}?track=${index}`,
    formatHint: 'wav',
  })

  try {
    await streamer.startStream({
      streamId,
      current: source(0),
      next: source(1),
      transport: rtpTransport(socket, 0x66778801),
      source: { http: { maxRetries: 0, cacheTempFiles: true, maxBytes: 1024 * 1024 } },
      buffer: { prebufferMs: 20, nextPrimeMs: 40 },
    })
    for (let index = 1; index < 12; index += 1) {
      await waitForStatus(
        () => streamer.getStatus(streamId),
        (status) => status.current?.id === `http-${index}` && status.playState === 'playing',
      ).catch(async (error: unknown) => {
        throw new Error(
          `http-${index} did not play: ${JSON.stringify({
            status: await streamer.getStatus(streamId),
            events: streamer.drainEvents(streamId),
          })}`,
          { cause: error },
        )
      })
      if (index + 1 < 12) await streamer.setNext(streamId, source(index + 1))
    }
    expect((await streamer.getStatus(streamId)).current?.id).toBe('http-11')
  } finally {
    await stopStreamIfPresent(streamer, streamId)
    await streamer.shutdown()
    await server.close()
    await closeSocket(socket)
  }
})
