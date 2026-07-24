import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from 'vitest'

import { Streamer } from '..'
import {
	closeSocket,
	createBoundUdpSocket,
	isRtpForSsrc,
	makeReceiverReport,
	makeSineWave,
	rtpTransport,
	send,
	stopStreamIfPresent,
	waitForDatagram,
	waitForStatus,
} from './helpers'

test('RTCP receiver reports update the asynchronous status snapshot', async () => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'music-rtcp-'))
	const wavPath = path.join(directory, 'rtcp.wav')
	await fs.promises.writeFile(wavPath, makeSineWave(1))
	const socket = await createBoundUdpSocket()
	const streamer = new Streamer()
	const streamId = `rtcp-${Date.now()}`
	const ssrc = 0x55667788

	try {
		const first = waitForDatagram(socket, (message) => isRtpForSsrc(message, ssrc))
		await streamer.startStream({
			streamId,
			current: {
				id: 'rtcp',
				attemptId: 'attempt-rtcp',
				kind: 'file',
				path: wavPath,
				seekable: true,
			},
			transport: rtpTransport(socket, ssrc),
		})
		const datagram = await first
		await send(
			socket,
			makeReceiverReport({ sourceSsrc: ssrc, fractionLost: 16, totalLost: 2, jitter: 480 }),
			datagram.rinfo.port,
			datagram.rinfo.address,
		)
		const status = await waitForStatus(
			() => streamer.getStatus(streamId),
			(value) => value.receiverReport !== undefined,
			2_000,
		)
		expect(status.receiverReport?.fractionLost).toBe(16)
		expect(status.receiverReport?.jitterMs).toBe(10)
	} finally {
		await stopStreamIfPresent(streamer, streamId)
		await closeSocket(socket)
		await fs.promises.rm(directory, { recursive: true, force: true })
	}
})
