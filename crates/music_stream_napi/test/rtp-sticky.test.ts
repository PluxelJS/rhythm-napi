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
		attemptId: `attempt-${index}`,
		kind: 'url' as const,
		url: `${server.url}?track=${index}`,
		formatHint: 'wav',
	})

	try {
		await streamer.startStream({
			streamId,
			current: source(0),
			transport: rtpTransport(socket, 0x66778801),
			source: { http: { maxRetries: 0, cacheTempFiles: true, maxBytes: 1024 * 1024 } },
			buffer: { prebufferMs: 20, nextPrimeMs: 40 },
		})
		await streamer.reconcilePlan(streamId, {
			version: 1,
			current: source(0),
			next: source(1),
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
			if (index + 1 < 12) {
				await streamer.reconcilePlan(streamId, {
					version: index + 1,
					current: source(index),
					next: source(index + 1),
				})
			}
		}
		expect((await streamer.getStatus(streamId)).current?.id).toBe('http-11')
	} finally {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await server.close()
		await closeSocket(socket)
	}
})

test('completed extensionless preloads cannot poison tempfile admission for later tracks', async () => {
	const server = await createHttpServerWorker(makeSineWave(0.3))
	const socket = await createBoundUdpSocket()
	const streamer = new Streamer({
		maxBlockingPreloads: 1,
		maxConcurrentHttpDownloads: 2,
		maxTempfileBytes: 4 * 1024 * 1024,
	})
	const streamId = `rtp-http-extensionless-${Date.now()}`
	const extensionlessUrl = new URL(server.url)
	extensionlessUrl.pathname = '/opaque-media'
	const source = (index: number) => ({
		id: `extensionless-${index}`,
		attemptId: `extensionless-attempt-${index}`,
		kind: 'url' as const,
		// Deliberately omit both a path extension and formatHint. Signed provider URLs such as
		// Netease can have this shape, which forces the complete-artifact decode path.
		url: `${extensionlessUrl}?opaque_track=${index}`,
	})

	try {
		await streamer.startStream({
			streamId,
			current: source(0),
			transport: rtpTransport(socket, 0x66778802),
			source: { http: { maxRetries: 0, cacheTempFiles: true, maxBytes: 1024 * 1024 } },
			buffer: { prebufferMs: 20, nextPrimeMs: 40 },
		})
		await streamer.reconcilePlan(streamId, {
			version: 1,
			current: source(0),
			next: source(1),
		})
		for (let index = 1; index < 4; index += 1) {
			await waitForStatus(
				() => streamer.getStatus(streamId),
				(status) =>
					status.current?.id === `extensionless-${index}` && status.playState === 'playing',
			).catch(async (error: unknown) => {
				throw new Error(
					`extensionless-${index} did not play: ${JSON.stringify({
						status: await streamer.getStatus(streamId),
						events: streamer.drainEvents(streamId),
					})}`,
					{ cause: error },
				)
			})
			const diagnostics = streamer.getResourceDiagnostics()
			expect(diagnostics.tempfilePreloadUnitsAvailable).toBe(1)
			// The regression must pass while completed files remain cached. Otherwise clearing the
			// cache could hide the role-permit leak instead of proving the terminal transition.
			expect(diagnostics.artifactCacheEntries).toBeGreaterThan(0)
			expect(diagnostics.artifactCacheRetainedQuotaBytes).toBeGreaterThan(0)
			if (index + 1 < 4) {
				await streamer.reconcilePlan(streamId, {
					version: index + 1,
					current: source(index),
					next: source(index + 1),
				})
			}
		}
		expect((await streamer.getStatus(streamId)).current?.id).toBe('extensionless-3')
	} finally {
		await stopStreamIfPresent(streamer, streamId)
		await streamer.shutdown()
		await server.close()
		await closeSocket(socket)
	}
})
