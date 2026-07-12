import type {
  ReplayGainInput,
  StreamEventOutput,
  StreamStatusBatchItemOutput,
  StreamStatusOutput,
  TrackSourceInput,
} from '..'

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2) ? true : false
type Assert<Value extends true> = Value

type _TrackKindContract = Assert<Equal<
  TrackSourceInput['kind'],
  'file' | 'url' | 'live'
>>
type _PlayStateContract = Assert<Equal<
  StreamStatusOutput['playState'],
  'idle' | 'buffering' | 'playing' | 'paused' | 'stopped'
>>
type _ReplayGainModeContract = Assert<Equal<
  ReplayGainInput['mode'],
  'track' | 'album' | undefined
>>
type _EventTypeContract = Assert<Equal<
  StreamEventOutput['type'],
  | 'streamStarted'
  | 'streamStopped'
  | 'stateChanged'
  | 'nextNeeded'
  | 'sourceRefreshNeeded'
  | 'networkQualityChanged'
  | 'error'
>>
type _ErrorCodeContract = Assert<Equal<
  NonNullable<StreamEventOutput['code']>,
  NonNullable<StreamStatusBatchItemOutput['code']>
>>
