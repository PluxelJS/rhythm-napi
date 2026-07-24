import type {
	ReplayGainInput,
	StreamEventOutput,
	StreamStatusBatchItemOutput,
	StreamStatusOutput,
	TrackSourceInput,
	TrackSourceOutput,
} from '..'

type Equal<Left, Right> =
	(<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
		? true
		: false
type Assert<Value extends true> = Value

type _TrackKindContract = Assert<Equal<TrackSourceInput['kind'], 'file' | 'url' | 'live'>>
type _AttemptIdentityContract = Assert<Equal<TrackSourceInput['attemptId'], string>>
type _AttemptOutputIdentityContract = Assert<Equal<TrackSourceOutput['attemptId'], string>>
type _PlayStateContract = Assert<
	Equal<StreamStatusOutput['playState'], 'idle' | 'buffering' | 'playing' | 'paused' | 'stopped'>
>
type _PlayoutDiagnosticsContract = Assert<
	Equal<
		StreamStatusOutput['playoutDiagnostics'],
		| {
				bufferedMs: number
				packetsSent: number
				bytesSent: number
				droppedFrames: number
				droppedMediaMs: number
				latencyRecoveries: number
				underruns: number
				maxLatenessMs: number
				sequence: number
				rtpTimestamp: number
		  }
		| undefined
	>
>
type _ReplayGainModeContract = Assert<Equal<ReplayGainInput['mode'], 'track' | 'album' | undefined>>
type _EventTypeContract = Assert<
	Equal<
		StreamEventOutput['type'],
		| 'streamStarted'
		| 'streamStopped'
		| 'stateChanged'
		| 'nextNeeded'
		| 'sourceRefreshNeeded'
		| 'networkQualityChanged'
		| 'attemptFailed'
		| 'error'
	>
>
type _SourceRoleContract = Assert<
	Equal<StreamEventOutput['sourceRole'], 'current' | 'next' | undefined>
>
type _ErrorCodeContract = Assert<
	Equal<NonNullable<StreamEventOutput['code']>, NonNullable<StreamStatusBatchItemOutput['code']>>
>
