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
	expect(() => new Streamer({ maxStreams: 0 })).toThrow(/resource|limit|invalid/i)
	expect(
		() =>
			new Streamer({
				maxBlockingProducers: 4,
				maxBlockingPreloads: 4,
			}),
	).toThrow(/resource|limit|invalid/i)
	expect(
		() =>
			new Streamer({
				maxConcurrentLiveStreams: 0,
			}),
	).toThrow(/resource|limit|invalid/i)
	expect(() =>
		streamer.validateSourceResolverConfig({
			http: { ioTimeoutMs: 0 },
		}),
	).toThrow(/greater than zero/)
	expect(() =>
		streamer.validateSourceResolverConfig({
			liveHttp: { retryBackoffMs: 0 },
		}),
	).toThrow(/non-zero/)
	expect(() =>
		streamer.validateSourceResolverConfig({
			liveHttp: { idleTimeoutMs: 0 },
		}),
	).toThrow(/greater than zero/)
	expect(() =>
		streamer.validateRtpTransportConfig({
			ip: '127.0.0.1',
			port: 0,
			audioSsrc: 1,
		}),
	).toThrow(/port/)
	expect(() =>
		streamer.validateRtpTransportConfig({
			ip: '127.0.0.1',
			port: 5_000,
			audioSsrc: 1,
			rtpKeepaliveIntervalMs: 0,
		}),
	).toThrow(/transport|invalid/i)
})

test('public-only sources reject custom headers at the N-API boundary', async () => {
	const socket = await createBoundUdpSocket()
	const streamer = new Streamer()

	try {
		await expect(
			streamer.startStream({
				streamId: `public-headers-${Date.now()}`,
				current: {
					id: 'untrusted-authenticated-url',
					attemptId: 'attempt-untrusted-authenticated-url',
					kind: 'live',
					url: 'https://1.1.1.1/live.mp3',
					headers: { authorization: 'Bearer secret' },
					networkPolicy: 'public-only',
				},
				transport: rtpTransport(socket, 0x33445566),
			}),
		).rejects.toThrow(/INVALID_SOURCE.*custom HTTP headers/i)
	} finally {
		await streamer.shutdown()
		await closeSocket(socket)
	}
})

test('HTTP authorization failure is reported asynchronously', async () => {
	const server = await createHttpStatusServerWorker(403)
	const socket = await createBoundUdpSocket()
	const streamer = new Streamer()
	const streamId = `auth-${Date.now()}`
	const callbackEvents: StreamEventOutput[] = []
	streamer.setEventCallback((event) => callbackEvents.push(event))

	try {
		await streamer.startStream({
			streamId,
			current: {
				id: 'auth',
				attemptId: 'attempt-auth',
				kind: 'url',
				url: `${server.url}?token=super-secret`,
				seekable: true,
			},
			transport: rtpTransport(socket, 0x44556677),
		})
		await expect(
			streamer.reconcilePlan(streamId, {
				version: 1,
				current: {
					id: 'auth',
					attemptId: 'attempt-auth',
					kind: 'url',
					url: `${server.url}?token=super-secret`,
					seekable: true,
				},
				next: {
					id: 'live-next',
					attemptId: 'attempt-live-next',
					kind: 'live',
					url: server.url,
				},
			}),
		).rejects.toThrow(/UNSUPPORTED.*timeshift/i)
		const events: StreamEventOutput[] = []
		const deadline = Date.now() + 2_000
		while (Date.now() < deadline && !events.some((event) => event.code === 'SOURCE_AUTH_EXPIRED')) {
			await new Promise((resolve) => setTimeout(resolve, 20))
			events.push(...streamer.drainEvents(streamId))
		}
		expect(
			events.some(
				(event) => event.type === 'sourceRefreshNeeded' && event.sourceRole === 'current',
			),
			JSON.stringify(events),
		).toBe(true)
		expect(
			events.some((event) => event.code === 'SOURCE_AUTH_EXPIRED'),
			JSON.stringify(events),
		).toBe(true)
		expect(events.every((event) => !event.message?.includes('super-secret'))).toBe(true)
		expect(events.every((event) => event.sequence > 0)).toBe(true)
		expect(new Set(events.map((event) => event.sequence)).size).toBe(events.length)
		expect(
			events.every((event, index) => index === 0 || event.sequence > events[index - 1].sequence),
		).toBe(true)
		const callbackDeadline = Date.now() + 500
		while (
			Date.now() < callbackDeadline &&
			!callbackEvents.some((callback) =>
				events.some((event) => event.sequence === callback.sequence),
			)
		) {
			await new Promise((resolve) => setTimeout(resolve, 10))
		}
		expect(
			callbackEvents.some((callback) =>
				events.some((event) => event.sequence === callback.sequence),
			),
		).toBe(true)
	} finally {
		streamer.setEventCallback(null)
		await streamer.shutdown()
		await server.close()
		await closeSocket(socket)
	}
})
