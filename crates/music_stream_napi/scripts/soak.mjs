import dgram from 'node:dgram'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { monitorEventLoopDelay, performance } from 'node:perf_hooks'

import native from '../index.js'

const { Streamer } = native
const options = parseOptions(process.argv.slice(2))
const abort = new AbortController()
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => abort.abort(new Error(`received ${signal}`)))
}

const temporaryDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'rhythm-soak-'))
const fixturePath = path.join(temporaryDirectory, 'quality-baseline-44100-stereo.wav')
await fs.writeFile(fixturePath, makeFixtureWav(options.fixtureSeconds))

const sink = dgram.createSocket('udp4')
let sinkPackets = 0
let sinkBytes = 0
let sinkRtpPackets = 0
let sinkRtpBytes = 0
let sinkRtcpPackets = 0
let sinkRtcpBytes = 0
sink.on('message', (message) => {
  sinkPackets += 1
  sinkBytes += message.length
  if (isRtcpPacket(message)) {
    sinkRtcpPackets += 1
    sinkRtcpBytes += message.length
  } else {
    sinkRtpPackets += 1
    sinkRtpBytes += message.length
  }
})
await new Promise((resolve, reject) => {
  sink.once('error', reject)
  sink.bind(0, '127.0.0.1', () => {
    sink.off('error', reject)
    resolve()
  })
})
const sinkPort = sink.address().port

const streamer = new Streamer({ maxStreams: options.streams })
const streamIds = Array.from({ length: options.streams }, (_, index) => `soak-${index}`)
const nextTrackIndex = new Map(streamIds.map((streamId) => [streamId, 1]))
let maintenanceErrors = 0
let runtimeErrors = 0
let previousSinkPackets = 0
let previousSinkBytes = 0
let previousSinkRtpPackets = 0
let previousSinkRtpBytes = 0
let previousSinkRtcpPackets = 0
let previousSinkRtcpBytes = 0
const previousDiagnostics = new Map()
const eventLoop = monitorEventLoopDelay({ resolution: 20 })
eventLoop.enable()

const baselineMemory = process.memoryUsage()
let samples = 0
let peakRssBytes = baselineMemory.rss
let peakHeapUsedBytes = baselineMemory.heapUsed
let lastRssBytes = baselineMemory.rss
let lastHeapUsedBytes = baselineMemory.heapUsed
let eventLoopP99MaxMs = 0
let eventLoopMaxMs = 0
let missingDiagnosticsSamples = 0
let peakMissingDiagnostics = 0
let startedAt = performance.now()
let previousSampleAt = startedAt
let lastSampleAt = startedAt
try {
  await Promise.all(streamIds.map((streamId, index) => streamer.startStream({
    streamId,
    current: track(streamId, 0),
    opusBitrateBps: 128_000,
    transport: {
      ip: '127.0.0.1',
      port: sinkPort,
      audioSsrc: 0x52000000 + index,
      rtcpMux: true,
      mtu: 1_200,
    },
    buffer: {
      decodeBatchMs: 80,
      encodedCapacityMs: 400,
      prebufferMs: 100,
      nextPrimeMs: 200,
      maxPlayoutLatenessMs: 100,
    },
  })))

  startedAt = performance.now()
  previousSampleAt = startedAt
  lastSampleAt = startedAt
  console.log(JSON.stringify({
    type: 'start',
    streams: options.streams,
    durationSeconds: options.durationSeconds,
    sampleSeconds: options.sampleSeconds,
    fixtureSeconds: options.fixtureSeconds,
    opusComplexity: 10,
    opusBitrate: 128_000,
  }))

  const deadline = performance.now() + options.durationSeconds * 1_000
  while (performance.now() < deadline && !abort.signal.aborted) {
    await sleep(Math.min(options.sampleSeconds * 1_000, deadline - performance.now()), abort.signal)
      .catch(() => {})
    const statuses = await streamer.getStatuses(streamIds)
    const maintenance = []
    const buffered = []
    let packets = 0
    let bytes = 0
    let underruns = 0
    let droppedFrames = 0
    let droppedMediaMs = 0
    let latencyRecoveries = 0
    let maxLatenessMs = 0
    let missingDiagnostics = 0
    let currentTracks = 0
    let nextTracks = 0
    const playStates = {
      idle: 0,
      buffering: 0,
      playing: 0,
      paused: 0,
      stopped: 0,
    }

    for (const item of statuses) {
      if (!item.ok || !item.status) {
        runtimeErrors += 1
        continue
      }
      const { status } = item
      playStates[status.playState] += 1
      if (status.current) currentTracks += 1
      if (status.next) nextTracks += 1
      if (status.current && !status.next) {
        const index = nextTrackIndex.get(item.streamId) ?? 2
        nextTrackIndex.set(item.streamId, index + 1)
        maintenance.push(
          streamer.reconcilePlan(item.streamId, {
            version: status.planVersion + 1,
            current: sourceFromStatus(status.current),
            next: track(item.streamId, index),
          }).catch(() => {
            maintenanceErrors += 1
          }),
        )
      } else if (!status.current && !status.next && status.playState !== 'stopped') {
        const index = nextTrackIndex.get(item.streamId) ?? 2
        nextTrackIndex.set(item.streamId, index + 1)
        maintenance.push(
          streamer.reconcilePlan(item.streamId, {
            version: status.planVersion + 1,
            current: track(item.streamId, index),
          }).catch(() => {
            maintenanceErrors += 1
          }),
        )
      }

      const current = status.playoutDiagnostics
      if (!current) {
        missingDiagnostics += 1
        continue
      }
      buffered.push(current.bufferedMs)
      maxLatenessMs = Math.max(maxLatenessMs, current.maxLatenessMs)
      const previous = previousDiagnostics.get(item.streamId)
      if (previous) {
        packets += nonNegativeDelta(current.packetsSent, previous.packetsSent)
        bytes += nonNegativeDelta(current.bytesSent, previous.bytesSent)
        underruns += nonNegativeDelta(current.underruns, previous.underruns)
        droppedFrames += nonNegativeDelta(current.droppedFrames, previous.droppedFrames)
        droppedMediaMs += nonNegativeDelta(current.droppedMediaMs, previous.droppedMediaMs)
        latencyRecoveries += nonNegativeDelta(
          current.latencyRecoveries,
          previous.latencyRecoveries,
        )
      }
      previousDiagnostics.set(item.streamId, current)
    }
    await Promise.all(maintenance)

    const events = streamer.drainEvents()
    runtimeErrors += events.filter((event) => event.type === 'error').length
    buffered.sort((left, right) => left - right)
    const memory = process.memoryUsage()
    const currentSinkPackets = sinkPackets
    const currentSinkBytes = sinkBytes
    const currentSinkRtpPackets = sinkRtpPackets
    const currentSinkRtpBytes = sinkRtpBytes
    const currentSinkRtcpPackets = sinkRtcpPackets
    const currentSinkRtcpBytes = sinkRtcpBytes
    const currentEventLoopP99Ms = nanosecondsToMilliseconds(eventLoop.percentile(99))
    const currentEventLoopMaxMs = nanosecondsToMilliseconds(eventLoop.max)
    const sampledAt = performance.now()
    const sampleDurationSeconds = (sampledAt - previousSampleAt) / 1_000
    const sinkRtpPacketDelta = currentSinkRtpPackets - previousSinkRtpPackets
    samples += 1
    peakRssBytes = Math.max(peakRssBytes, memory.rss)
    peakHeapUsedBytes = Math.max(peakHeapUsedBytes, memory.heapUsed)
    lastRssBytes = memory.rss
    lastHeapUsedBytes = memory.heapUsed
    eventLoopP99MaxMs = Math.max(eventLoopP99MaxMs, currentEventLoopP99Ms)
    eventLoopMaxMs = Math.max(eventLoopMaxMs, currentEventLoopMaxMs)
    missingDiagnosticsSamples += missingDiagnostics
    peakMissingDiagnostics = Math.max(peakMissingDiagnostics, missingDiagnostics)
    const sample = {
      type: 'sample',
      elapsedSeconds: round((sampledAt - startedAt) / 1_000),
      rssMiB: round(memory.rss / 1024 / 1024),
      heapUsedMiB: round(memory.heapUsed / 1024 / 1024),
      eventLoopMeanMs: nanosecondsToMilliseconds(eventLoop.mean),
      eventLoopP99Ms: currentEventLoopP99Ms,
      eventLoopMaxMs: currentEventLoopMaxMs,
      bufferedMs: summarize(buffered),
      packets,
      bytes,
      sinkPackets: currentSinkPackets - previousSinkPackets,
      sinkBytes: currentSinkBytes - previousSinkBytes,
      sinkRtpPackets: sinkRtpPacketDelta,
      sinkRtpBytes: currentSinkRtpBytes - previousSinkRtpBytes,
      rtpPacketCoverageRatio: packetCoverageRatio(
        sinkRtpPacketDelta,
        options.streams,
        sampleDurationSeconds,
      ),
      sinkRtcpPackets: currentSinkRtcpPackets - previousSinkRtcpPackets,
      sinkRtcpBytes: currentSinkRtcpBytes - previousSinkRtcpBytes,
      underruns,
      droppedFrames,
      droppedMediaMs,
      latencyRecoveries,
      maxLatenessMs,
      missingDiagnostics,
      playStates,
      currentTracks,
      nextTracks,
      maintenanceErrors,
      runtimeErrors,
    }
    console.log(JSON.stringify(sample))
    previousSinkPackets = currentSinkPackets
    previousSinkBytes = currentSinkBytes
    previousSinkRtpPackets = currentSinkRtpPackets
    previousSinkRtpBytes = currentSinkRtpBytes
    previousSinkRtcpPackets = currentSinkRtcpPackets
    previousSinkRtcpBytes = currentSinkRtcpBytes
    previousSampleAt = sampledAt
    lastSampleAt = sampledAt
    eventLoop.reset()
    if (deadline - performance.now() <= 10) break
  }
} finally {
  eventLoop.disable()
  await streamer.shutdown().catch(() => {
    runtimeErrors += 1
  })
  await new Promise((resolve) => sink.close(resolve))
  await fs.rm(temporaryDirectory, { recursive: true, force: true })
}

const diagnostics = sumDiagnostics(previousDiagnostics.values())
const measuredSeconds = (lastSampleAt - startedAt) / 1_000
console.log(JSON.stringify({
  type: 'summary',
  elapsedSeconds: round((performance.now() - startedAt) / 1_000),
  samples,
  sinkPackets,
  sinkBytes,
  sinkRtpPackets,
  sinkRtpBytes,
  rtpPacketCoverageRatio: packetCoverageRatio(sinkRtpPackets, options.streams, measuredSeconds),
  sinkRtcpPackets,
  sinkRtcpBytes,
  diagnosticsStreams: previousDiagnostics.size,
  ...diagnostics,
  rssBaselineMiB: bytesToMebibytes(baselineMemory.rss),
  rssPeakMiB: bytesToMebibytes(peakRssBytes),
  rssLastSampleMiB: bytesToMebibytes(lastRssBytes),
  rssGrowthMiB: bytesToMebibytes(lastRssBytes - baselineMemory.rss),
  heapUsedBaselineMiB: bytesToMebibytes(baselineMemory.heapUsed),
  heapUsedPeakMiB: bytesToMebibytes(peakHeapUsedBytes),
  heapUsedLastSampleMiB: bytesToMebibytes(lastHeapUsedBytes),
  eventLoopP99MaxMs,
  eventLoopMaxMs,
  missingDiagnosticsSamples,
  peakMissingDiagnostics,
  maintenanceErrors,
  runtimeErrors,
  interrupted: abort.signal.aborted,
}))
if (runtimeErrors > 0) process.exitCode = 1

function track(streamId, index) {
  return {
    attemptId: `${streamId}:attempt:${index}`,
    id: `${streamId}:fixture:${index}`,
    kind: 'file',
    path: fixturePath,
    formatHint: 'wav',
    seekable: true,
  }
}

function sourceFromStatus(source) {
  return {
    attemptId: source.attemptId,
    id: source.id,
    kind: source.kind,
    path: fixturePath,
    formatHint: source.formatHint ?? 'wav',
    seekable: source.seekable ?? true,
  }
}

function parseOptions(arguments_) {
  const values = new Map()
  for (let index = 0; index < arguments_.length; index += 2) {
    const name = arguments_[index]
    const value = arguments_[index + 1]
    if (!name?.startsWith('--') || value === undefined) usage(`invalid argument: ${name ?? ''}`)
    values.set(name.slice(2), value)
  }
  const options = {
    streams: integerOption(values, 'streams', 10, 1, 500),
    durationSeconds: integerOption(values, 'duration-seconds', 120, 5, 86_400),
    sampleSeconds: integerOption(values, 'sample-seconds', 2, 1, 60),
    fixtureSeconds: integerOption(values, 'fixture-seconds', 5, 2, 60),
  }
  if (options.sampleSeconds * 2 > options.fixtureSeconds) {
    usage('--sample-seconds must be at most half of --fixture-seconds so next stays primed')
  }
  return options
}

function integerOption(values, name, fallback, minimum, maximum) {
  const value = Number(values.get(name) ?? fallback)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    usage(`--${name} must be an integer between ${minimum} and ${maximum}`)
  }
  return value
}

function usage(message) {
  throw new TypeError(
    `${message}\nusage: node scripts/soak.mjs --streams 10 --duration-seconds 120 `
      + '--sample-seconds 2 --fixture-seconds 5',
  )
}

function makeFixtureWav(seconds) {
  const sampleRate = 44_100
  const channels = 2
  const frames = sampleRate * seconds
  const dataBytes = frames * channels * 2
  const wav = Buffer.allocUnsafe(44 + dataBytes)
  wav.write('RIFF', 0)
  wav.writeUInt32LE(36 + dataBytes, 4)
  wav.write('WAVEfmt ', 8)
  wav.writeUInt32LE(16, 16)
  wav.writeUInt16LE(1, 20)
  wav.writeUInt16LE(channels, 22)
  wav.writeUInt32LE(sampleRate, 24)
  wav.writeUInt32LE(sampleRate * channels * 2, 28)
  wav.writeUInt16LE(channels * 2, 32)
  wav.writeUInt16LE(16, 34)
  wav.write('data', 36)
  wav.writeUInt32LE(dataBytes, 40)
  for (let frame = 0; frame < frames; frame += 1) {
    const time = frame / sampleRate
    const envelope = 0.55 + 0.35 * Math.sin(2 * Math.PI * 0.5 * time)
    const transient = frame % 11_025 < 220 ? 0.18 * Math.exp(-(frame % 11_025) / 55) : 0
    const left = envelope * (
      0.38 * Math.sin(2 * Math.PI * 220 * time)
      + 0.22 * Math.sin(2 * Math.PI * 1_760 * time)
    ) + transient
    const right = envelope * (
      0.35 * Math.sin(2 * Math.PI * 329.63 * time)
      + 0.2 * Math.sin(2 * Math.PI * 2_640 * time)
    ) - transient
    wav.writeInt16LE(toPcm16(left), 44 + frame * 4)
    wav.writeInt16LE(toPcm16(right), 46 + frame * 4)
  }
  return wav
}

function toPcm16(value) {
  return Math.round(Math.max(-1, Math.min(1, value)) * 32_767)
}

function nonNegativeDelta(current, previous) {
  return Math.max(0, current - previous)
}

function isRtcpPacket(message) {
  if (message.length < 2) return false
  const packetType = message[1]
  return packetType >= 192 && packetType <= 223
}

function sumDiagnostics(values) {
  const totals = {
    packetsSent: 0,
    bytesSent: 0,
    underruns: 0,
    droppedFrames: 0,
    droppedMediaMs: 0,
    latencyRecoveries: 0,
    maxLatenessMs: 0,
  }
  for (const value of values) {
    totals.packetsSent += value.packetsSent
    totals.bytesSent += value.bytesSent
    totals.underruns += value.underruns
    totals.droppedFrames += value.droppedFrames
    totals.droppedMediaMs += value.droppedMediaMs
    totals.latencyRecoveries += value.latencyRecoveries
    totals.maxLatenessMs = Math.max(totals.maxLatenessMs, value.maxLatenessMs)
  }
  return totals
}

function packetCoverageRatio(packets, streams, seconds) {
  if (seconds <= 0) return 0
  return round(packets / (streams * seconds * 50))
}

function summarize(values) {
  if (values.length === 0) return undefined
  return {
    min: values[0],
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    max: values.at(-1),
  }
}

function percentile(sorted, percentile_) {
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * percentile_))]
}

function nanosecondsToMilliseconds(value) {
  return Number.isFinite(value) ? round(value / 1_000_000) : 0
}

function round(value) {
  return Math.round(value * 1_000) / 1_000
}

function bytesToMebibytes(value) {
  return round(value / 1024 / 1024)
}

function sleep(milliseconds, signal) {
  if (milliseconds <= 0) return Promise.resolve()
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal.reason)
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, milliseconds)
    signal.addEventListener('abort', onAbort, { once: true })
  })
}
