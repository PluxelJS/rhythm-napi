import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { afterEach, expect, test } from 'vitest'
import { Streamer, type ExternalOpusFrameAckInput } from '..'
import { delay, makeSineWave, stopStreamIfPresent, waitForStatus } from './helpers'

const resources: Array<() => Promise<void>> = []

afterEach(async () => {
	await Promise.allSettled(resources.splice(0).map((dispose) => dispose()))
})

test('external pull delivers paced Opus frames and commits progress with the next read', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-pull-'))
	const audioPath = path.join(directory, 'audio.wav')
	await fs.promises.writeFile(audioPath, makeSineWave(0.4))
	const streamer = new Streamer()
	const streamId = `external-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	const started = await streamer.startExternalStream({
		streamId,
		current: { id: 'external', attemptId: 'attempt-external', kind: 'file', path: audioPath },
		opusBitrateBps: 128_000,
		buffer: { prebufferMs: 20, encodedCapacityMs: 400, maxPlayoutLatenessMs: 40 },
	})
	expect(started.streamId).toBe(streamId)

	const first = await streamer.pullExternalFrame(streamId)
	expect(first).not.toBeNull()
	expect(Buffer.isBuffer(first!.payload)).toBe(true)
	expect(first!.payload.length).toBeGreaterThan(0)
	expect(first!.samplesPerChannel).toBe(960)
	expect(first!.deadlineMonotonicNs).toBeGreaterThan(process.hrtime.bigint())

	const firstAck: ExternalOpusFrameAckInput = {
		leaseId: first!.leaseId,
		generation: first!.generation,
		outcome: 'sent',
	}
	const beforeSecond = process.hrtime.bigint()
	const second = await streamer.pullExternalFrame(streamId, firstAck)
	const elapsedMs = Number(process.hrtime.bigint() - beforeSecond) / 1_000_000
	expect(second).not.toBeNull()
	expect(elapsedMs).toBeGreaterThanOrEqual(8)
	expect(second!.mediaPositionMs).toBeGreaterThan(first!.mediaPositionMs)

	const afterFirst = await streamer.getStatus(streamId)
	expect(afterFirst.timePlayedMs).toBeGreaterThanOrEqual(20)
	await delay(120)
	const third = await streamer.pullExternalFrame(streamId, {
		leaseId: second!.leaseId,
		generation: second!.generation,
		outcome: 'sent',
	})
	expect(third).not.toBeNull()
	const beforeFourth = process.hrtime.bigint()
	const fourth = await streamer.pullExternalFrame(streamId, {
		leaseId: third!.leaseId,
		generation: third!.generation,
		outcome: 'sent',
	})
	const recoveredIntervalMs = Number(process.hrtime.bigint() - beforeFourth) / 1_000_000
	expect(fourth).not.toBeNull()
	expect(recoveredIntervalMs).toBeGreaterThanOrEqual(8)
	await streamer.finishExternalFrame(streamId, {
		leaseId: fourth!.leaseId,
		generation: fourth!.generation,
		outcome: 'sent',
	})
	const recovered = await streamer.getStatus(streamId)
	expect(recovered.timePlayedMs).toBeGreaterThanOrEqual(80)
	expect(recovered.playoutDiagnostics?.packetsSent).toBe(4)
	expect(recovered.playoutDiagnostics?.droppedFrames).toBeGreaterThan(0)
	expect(recovered.playoutDiagnostics?.latencyRecoveries).toBeGreaterThan(0)
})

test('external pull bounds concurrent reads and cancellation wakes a pending reader', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-cancel-'))
	const audioPath = path.join(directory, 'audio.wav')
	await fs.promises.writeFile(audioPath, makeSineWave(0.4))
	const streamer = new Streamer()
	const streamId = `external-cancel-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: {
			id: 'external-cancel',
			attemptId: 'attempt-external-cancel',
			kind: 'file',
			path: audioPath,
		},
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	await streamer.pauseStream(streamId)
	const pending = streamer.pullExternalFrame(streamId)
	await delay(10)
	await expect(streamer.pullExternalFrame(streamId)).rejects.toThrow(/one external pull|pending/i)
	await streamer.cancelExternalPull(streamId)
	await expect(pending).resolves.toBeNull()
})

test('external pull accepts the retired lease after seek and continues on the new generation', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-seek-'))
	const audioPath = path.join(directory, 'audio.wav')
	await fs.promises.writeFile(audioPath, makeSineWave(2))
	const streamer = new Streamer()
	const streamId = `external-seek-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: {
			id: 'external-seek',
			attemptId: 'attempt-external-seek',
			kind: 'file',
			path: audioPath,
			seekable: true,
		},
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	const beforeSeek = await streamer.pullExternalFrame(streamId)
	const seeked = await streamer.seekStream(streamId, 1)
	await expect(streamer.pullExternalFrame(streamId)).rejects.toThrow(/previous.*outstanding/i)
	const afterSeek = await streamer.pullExternalFrame(streamId, {
		leaseId: beforeSeek!.leaseId,
		generation: beforeSeek!.generation,
		outcome: 'sent',
	})

	expect(afterSeek).not.toBeNull()
	expect(afterSeek!.generation).toBe(seeked.generation)
	expect(afterSeek!.generation).not.toBe(beforeSeek!.generation)
	await streamer.finishExternalFrame(streamId, {
		leaseId: afterSeek!.leaseId,
		generation: afterSeek!.generation,
		outcome: 'sent',
	})
	expect((await streamer.getStatus(streamId)).playoutDiagnostics?.packetsSent).toBe(2)
})

test('external pull stays parked while current is absent and resumes after a later plan', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-idle-'))
	const firstPath = path.join(directory, 'first.wav')
	const secondPath = path.join(directory, 'second.wav')
	await Promise.all([
		fs.promises.writeFile(firstPath, makeSineWave(1)),
		fs.promises.writeFile(secondPath, makeSineWave(1)),
	])
	const streamer = new Streamer()
	const streamId = `external-idle-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: { id: 'first', attemptId: 'attempt-first', kind: 'file', path: firstPath },
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	const first = await streamer.pullExternalFrame(streamId)
	await streamer.reconcilePlan(streamId, { version: 1 })
	const pending = streamer.pullExternalFrame(streamId, {
		leaseId: first!.leaseId,
		generation: first!.generation,
		outcome: 'cancelled',
	})
	await delay(20)
	await streamer.reconcilePlan(streamId, {
		version: 2,
		current: { id: 'second', attemptId: 'attempt-second', kind: 'file', path: secondPath },
	})
	const resumed = await pending

	expect(resumed).not.toBeNull()
	expect(resumed!.generation).not.toBe(first!.generation)
	await streamer.finishExternalFrame(streamId, {
		leaseId: resumed!.leaseId,
		generation: resumed!.generation,
		outcome: 'sent',
	})
})

test('external output unavailability is a stream error, not a track attempt failure', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-failure-'))
	const audioPath = path.join(directory, 'audio.wav')
	await fs.promises.writeFile(audioPath, makeSineWave(1))
	const streamer = new Streamer()
	const streamId = `external-failure-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: { id: 'failure', attemptId: 'attempt-failure', kind: 'file', path: audioPath },
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	const frame = await streamer.pullExternalFrame(streamId)
	await streamer.finishExternalFrame(streamId, {
		leaseId: frame!.leaseId,
		generation: frame!.generation,
		outcome: 'outputUnavailable',
	})
	await waitForStatus(
		() => streamer.getStatus(streamId),
		(status) => status.playState === 'stopped',
	)
	const events = streamer.drainEvents(streamId)

	expect(events).toContainEqual(
		expect.objectContaining({ type: 'error', code: 'OUTPUT_ERROR' }),
	)
	expect(events.every((event) => event.type !== 'attemptFailed')).toBe(true)
	expect(events.every((event) => event.type !== 'nextNeeded')).toBe(true)
})

test('an abandoned retired frame lease fails the output within a fixed deadline', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-timeout-'))
	const audioPath = path.join(directory, 'audio.wav')
	await fs.promises.writeFile(audioPath, makeSineWave(3))
	const streamer = new Streamer()
	const streamId = `external-timeout-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: {
			id: 'timeout',
			attemptId: 'attempt-timeout',
			kind: 'file',
			path: audioPath,
			seekable: true,
		},
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	await streamer.pullExternalFrame(streamId)
	await streamer.seekStream(streamId, 1)
	await waitForStatus(
		() => streamer.getStatus(streamId),
		(status) => status.playState === 'stopped',
		3_000,
	)
	const events = streamer.drainEvents(streamId)

	expect(events).toContainEqual(
		expect.objectContaining({ type: 'error', code: 'OUTPUT_ERROR' }),
	)
	expect(events.every((event) => event.type !== 'attemptFailed')).toBe(true)
})

test('external pull keeps a pending read alive across repeated automatic promotion', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-external-promotion-'))
	const paths = await Promise.all(
		['a', 'b', 'c'].map(async (id) => {
			const audioPath = path.join(directory, `${id}.wav`)
			await fs.promises.writeFile(audioPath, makeSineWave(0.12))
			return audioPath
		}),
	)
	const streamer = new Streamer()
	const streamId = `external-promotion-${Date.now()}`
	resources.push(async () => {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await fs.promises.rm(directory, { recursive: true, force: true })
	})

	await streamer.startExternalStream({
		streamId,
		current: { id: 'a', attemptId: 'attempt-a', kind: 'file', path: paths[0] },
		buffer: { prebufferMs: 20, encodedCapacityMs: 400 },
	})
	await streamer.reconcilePlan(streamId, {
		version: 1,
		current: { id: 'a', attemptId: 'attempt-a', kind: 'file', path: paths[0] },
		next: { id: 'b', attemptId: 'attempt-b', kind: 'file', path: paths[1] },
	})

	let previous: ExternalOpusFrameAckInput | undefined
	let bGeneration: number | undefined
	let cGeneration: number | undefined
	for (let pulls = 0; pulls < 30 && cGeneration === undefined; pulls += 1) {
		const frame = await streamer.pullExternalFrame(streamId, previous)
		expect(frame, 'a track boundary must not terminate the output').not.toBeNull()
		previous = {
			leaseId: frame!.leaseId,
			generation: frame!.generation,
			outcome: 'sent',
		}

		const status = await streamer.getStatus(streamId)
		if (status.current?.id === 'b' && bGeneration === undefined) {
			bGeneration = frame!.generation
			await streamer.reconcilePlan(streamId, {
				version: 2,
				current: { id: 'b', attemptId: 'attempt-b', kind: 'file', path: paths[1] },
				next: { id: 'c', attemptId: 'attempt-c', kind: 'file', path: paths[2] },
			})
		}
		if (status.current?.id === 'c') cGeneration = frame!.generation
	}

	expect(bGeneration).toBeDefined()
	expect(cGeneration).toBeDefined()
	expect(cGeneration).not.toBe(bGeneration)
	await streamer.finishExternalFrame(streamId, previous!)
	await waitForStatus(
		() => streamer.getStatus(streamId),
		(status) => status.current?.id === 'c' && status.playState === 'playing',
	)
})
