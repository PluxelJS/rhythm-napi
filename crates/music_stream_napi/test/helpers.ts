import dgram, { type RemoteInfo, type Socket } from 'node:dgram'
import { Worker } from 'node:worker_threads'

import type {
  RtpTransportConfigInput,
  Streamer,
} from '..'

export interface HttpServerWorker {
  url: string
  close: () => Promise<void>
}

export interface Datagram {
  message: Buffer
  rinfo: RemoteInfo
}

type HttpWorkerMessage =
  | { type: 'listening'; port: number }
  | { type: 'closed'; error: string | null }

export function makeSineWave(seconds = 0.4): Buffer {
  const sampleRate = 48_000
  const channels = 2
  const samples = Math.floor(sampleRate * seconds)
  const dataBytes = samples * channels * 2
  const buffer = Buffer.alloc(44 + dataBytes)

  buffer.write('RIFF', 0)
  buffer.writeUInt32LE(36 + dataBytes, 4)
  buffer.write('WAVE', 8)
  buffer.write('fmt ', 12)
  buffer.writeUInt32LE(16, 16)
  buffer.writeUInt16LE(1, 20)
  buffer.writeUInt16LE(channels, 22)
  buffer.writeUInt32LE(sampleRate, 24)
  buffer.writeUInt32LE(sampleRate * channels * 2, 28)
  buffer.writeUInt16LE(channels * 2, 32)
  buffer.writeUInt16LE(16, 34)
  buffer.write('data', 36)
  buffer.writeUInt32LE(dataBytes, 40)

  for (let sample = 0; sample < samples; sample += 1) {
    const value = Math.round(Math.sin((2 * Math.PI * 440 * sample) / sampleRate) * 12_000)
    for (let channel = 0; channel < channels; channel += 1) {
      buffer.writeInt16LE(value, 44 + (sample * channels + channel) * 2)
    }
  }

  return buffer
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export function makeReceiverReport({
  sourceSsrc,
  fractionLost,
  totalLost,
  jitter,
}: {
  sourceSsrc: number
  fractionLost: number
  totalLost: number
  jitter: number
}): Buffer {
  const buffer = Buffer.alloc(32)
  buffer[0] = 0x81
  buffer[1] = 201
  buffer.writeUInt16BE(7, 2)
  buffer.writeUInt32BE(0x01020304, 4)
  buffer.writeUInt32BE(sourceSsrc, 8)
  buffer[12] = fractionLost
  buffer[13] = (totalLost >>> 16) & 0xff
  buffer[14] = (totalLost >>> 8) & 0xff
  buffer[15] = totalLost & 0xff
  buffer.writeUInt32BE(4, 16)
  buffer.writeUInt32BE(jitter, 20)
  buffer.writeUInt32BE(0, 24)
  buffer.writeUInt32BE(0, 28)
  return buffer
}

export function isRtpForSsrc(message: Buffer, ssrc: number): boolean {
  return message.length >= 12 && message[1] !== 200 && message[1] !== 201 && message.readUInt32BE(8) === ssrc
}

export function rtpTransport(
  socket: Socket,
  ssrc: number,
  overrides: Partial<RtpTransportConfigInput> = {},
): RtpTransportConfigInput {
  return {
    ip: '127.0.0.1',
    port: socketPort(socket),
    audioSsrc: ssrc,
    audioPt: 111,
    rtcpMux: true,
    localIp: '127.0.0.1',
    localPort: 0,
    ...overrides,
  }
}

export async function createBoundUdpSocket(): Promise<Socket> {
  const socket = dgram.createSocket('udp4')
  await bind(socket, '127.0.0.1')
  return socket
}

function bind(socket: Socket, host: string): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.once('error', reject)
    socket.bind(0, host, () => {
      socket.off('error', reject)
      resolve()
    })
  })
}

export function createHttpServerWorker(
  body: Buffer,
  responseHeaders: Record<string, string> = {},
): Promise<HttpServerWorker> {
  const code = `
    const http = require('node:http')
    const { parentPort, workerData } = require('node:worker_threads')
    const body = Buffer.from(workerData.body)
    const server = http.createServer((_request, response) => {
      response.writeHead(200, {
        'content-length': body.length,
        'content-type': 'audio/wav',
        ...workerData.responseHeaders,
      })
      response.end(body)
    })
    server.listen(0, '127.0.0.1', () => {
      parentPort.postMessage({ type: 'listening', port: server.address().port })
    })
    parentPort.on('message', (message) => {
      if (message === 'close') {
        server.close((error) => {
          parentPort.postMessage({ type: 'closed', error: error ? error.message : null })
        })
      }
    })
  `
  return createHttpWorker(code, { body, responseHeaders }, 'tone.wav')
}

export function createHttpStatusServerWorker(statusCode: number): Promise<HttpServerWorker> {
  const code = `
    const http = require('node:http')
    const { parentPort, workerData } = require('node:worker_threads')
    const server = http.createServer((_request, response) => {
      response.writeHead(workerData.statusCode, {
        'content-length': 0,
        'content-type': 'text/plain',
      })
      response.end()
    })
    server.listen(0, '127.0.0.1', () => {
      parentPort.postMessage({ type: 'listening', port: server.address().port })
    })
    parentPort.on('message', (message) => {
      if (message === 'close') {
        server.close((error) => {
          parentPort.postMessage({ type: 'closed', error: error ? error.message : null })
        })
      }
    })
  `
  return createHttpWorker(code, { statusCode }, 'auth.wav')
}

function createHttpWorker(
  code: string,
  workerData: Record<string, unknown>,
  path: string,
): Promise<HttpServerWorker> {
  return new Promise<HttpServerWorker>((resolve, reject) => {
    const worker = new Worker(code, { eval: true, workerData })
    let closePromise: Promise<void> | undefined
    const onError = (error: Error) => {
      reject(error)
    }
    worker.once('error', onError)
    worker.on('message', (message: HttpWorkerMessage) => {
      if (message.type !== 'listening') {
        return
      }
      worker.off('error', onError)
      resolve({
        url: `http://127.0.0.1:${message.port}/${path}`,
        close: () => {
          closePromise ??= closeHttpServerWorker(worker)
          return closePromise
        },
      })
    })
  })
}

function closeHttpServerWorker(worker: Worker): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      cleanup()
      reject(error)
    }
    const onMessage = (message: HttpWorkerMessage) => {
      if (message.type !== 'closed') {
        return
      }
      cleanup()
      if (message.error) reject(new Error(message.error))
      else resolve()
    }
    const cleanup = () => {
      worker.off('error', onError)
      worker.off('message', onMessage)
    }
    worker.on('error', onError)
    worker.on('message', onMessage)
    worker.postMessage('close')
  }).finally(() => {
    void worker.terminate()
  })
}

export function waitForDatagram(
  socket: Socket,
  predicate: (message: Buffer, rinfo: RemoteInfo) => boolean,
  timeoutMs = 2_000,
): Promise<Datagram> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error('timed out waiting for UDP datagram'))
    }, timeoutMs)
    const onMessage = (message: Buffer, rinfo: RemoteInfo) => {
      if (!predicate(message, rinfo)) {
        return
      }
      cleanup()
      resolve({ message, rinfo })
    }
    const onError = (error: Error) => {
      cleanup()
      reject(error)
    }
    const cleanup = () => {
      clearTimeout(timer)
      socket.off('message', onMessage)
      socket.off('error', onError)
    }
    socket.on('message', onMessage)
    socket.on('error', onError)
  })
}

export function waitForStatus<T>(
  read: () => T | Promise<T>,
  predicate: (status: T) => boolean,
  timeoutMs = 1_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    const poll = async () => {
      try {
        const status = await read()
        if (predicate(status)) {
          resolve(status)
          return
        }
      } catch (error) {
        reject(error)
        return
      }
      if (Date.now() >= deadline) {
        reject(new Error('timed out waiting for status'))
        return
      }
      setTimeout(() => void poll(), 20)
    }
    void poll()
  })
}

export function waitForEvent<T>(
  events: T[],
  predicate: (event: T) => boolean,
  timeoutMs = 1_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    const poll = () => {
      const event = events.find(predicate)
      if (event) {
        resolve(event)
        return
      }
      if (Date.now() >= deadline) {
        reject(new Error('timed out waiting for event callback'))
        return
      }
      setTimeout(poll, 10)
    }
    poll()
  })
}

export function send(socket: Socket, buffer: Buffer, port: number, host: string): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.send(buffer, port, host, (error) => {
      if (error) reject(error)
      else resolve()
    })
  })
}

export function closeSocket(socket: Socket): Promise<void> {
  return new Promise<void>((resolve) => {
    socket.close(resolve)
  })
}

export async function stopStreamIfPresent(streamer: Streamer, streamId: string): Promise<void> {
  try {
    await streamer.stopStream(streamId)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    if (!message.includes('not found') && !message.includes('shut down')) {
      throw error
    }
  }
}

function socketPort(socket: Socket): number {
  const address = socket.address()
  if (typeof address === 'string') {
    throw new Error(`expected UDP address info, got ${address}`)
  }
  return address.port
}
