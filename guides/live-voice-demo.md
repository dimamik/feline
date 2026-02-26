# Live Voice Demo — In Depth

This guide walks through how `mix feline.talk` works: how microphone audio
flows through the pipeline, gets transcribed, generates an LLM response,
synthesizes speech, and plays it back — all in real time.

## The Pipeline

```
Mic (ffmpeg Port)
  │
  ▼  InputAudioRawFrame (16kHz, mono, 16-bit PCM)
VADProcessor
  │
  ▼  InputAudioRawFrame + UserStartedSpeakingFrame / UserStoppedSpeakingFrame
Deepgram.StreamingSTT
  │
  ▼  TranscriptionFrame
ConsoleLogger.UserInput ─── prints "You: ..."
  │
  ▼  TranscriptionFrame
UserContextAggregator ─── appends user message to LLM context, pushes LLMContextFrame
  │
  ▼  LLMContextFrame
OpenAI.StreamingLLM
  │
  ▼  LLMTextFrame (one per token) + LLMFullResponseStartFrame / LLMFullResponseEndFrame
AssistantContextAggregator ─── appends assistant message to LLM context
  │
  ▼  LLMTextFrame
ConsoleLogger.BotOutput ─── prints "Bot: ..." token by token
  │
  ▼  LLMTextFrame
SentenceAggregator ─── buffers tokens, emits TextFrame per complete sentence
  │
  ▼  TextFrame (one per sentence)
ElevenLabs.StreamingTTS ─── WebSocket streaming, emits audio chunks
  │
  ▼  TTSAudioRawFrame (24kHz, mono, 16-bit PCM)
AudioPlayer ─── buffers audio, plays via sox on TTSStoppedFrame
  │
  ▼  BotStartedSpeakingFrame / BotStoppedSpeakingFrame (upstream)
```

## Stage by Stage

### 1. Microphone Capture

The Mix task opens an ffmpeg process as an Erlang Port:

```
ffmpeg -f avfoundation -i :0 -ac 1 -ar 16000 -f s16le pipe:1
```

This captures the default macOS microphone and outputs raw PCM to stdout.
The task reads binary data from the port and chunks it into 640-byte frames
(20ms at 16kHz, mono, 16-bit). Each chunk becomes an `InputAudioRawFrame`
injected into the pipeline via `Pipeline.Task.queue_frame/2`.

See `lib/mix/tasks/feline.talk.ex`.

### 2. Voice Activity Detection

`VADProcessor` runs energy-based VAD on each audio chunk. It maintains a
state machine with two states: `:quiet` and `:speaking`.

- When energy exceeds the threshold for `start_secs` (0.2s), it emits
  `UserStartedSpeakingFrame`.
- When energy drops below the threshold for `stop_secs` (0.8s), it emits
  `UserStoppedSpeakingFrame`.

**Echo suppression**: When `bot_speaking` is true (set by
`BotStartedSpeakingFrame` flowing upstream from AudioPlayer), the VAD drops
all `InputAudioRawFrame`s. This prevents the bot from hearing its own voice
through the speakers.

See `lib/feline/processors/vad_processor.ex`.

### 3. Speech-to-Text (Deepgram)

`Deepgram.StreamingSTT` maintains a WebSocket connection to Deepgram's
real-time transcription API. It forwards `InputAudioRawFrame` audio bytes
over the socket and receives JSON transcription results back. Final
transcriptions become `TranscriptionFrame`s.

See `lib/feline/services/deepgram/streaming_stt.ex`.

### 4. Context Management

Two processors work as a pair to manage LLM conversation history:

- **`UserContextAggregator`** absorbs `TranscriptionFrame`, appends the
  user's message to a shared context (via an Agent process), and pushes
  `LLMContextFrame` containing the full conversation history.
- **`AssistantContextAggregator`** collects `LLMTextFrame` tokens into the
  assistant's response and appends it to the shared context when
  `LLMFullResponseEndFrame` arrives.

The shared context Agent (`ContextAggregatorPair`) ensures both aggregators
see the same conversation history.

See `lib/feline/processors/user_context_aggregator.ex` and
`lib/feline/processors/context_aggregator_pair.ex`.

### 5. LLM (OpenAI)

`OpenAI.StreamingLLM` receives `LLMContextFrame`, calls the OpenAI Chat
Completions API with `stream: true`, and pushes one `LLMTextFrame` per
token. The streaming request runs in a supervised task so it doesn't block
the processor's GenServer.

The response is bookended by `LLMFullResponseStartFrame` and
`LLMFullResponseEndFrame`.

See `lib/feline/services/openai/streaming_llm.ex`.

### 6. Sentence Aggregation

`SentenceAggregator` buffers `LLMTextFrame` tokens until a sentence
boundary (`.`, `!`, `?`) is found, then pushes the complete sentence as a
`TextFrame`. This allows TTS to start synthesizing as soon as the first
sentence is ready, without waiting for the entire LLM response.

On `LLMFullResponseEndFrame`, any remaining buffered text is flushed.

See `lib/feline/processors/sentence_aggregator.ex`.

### 7. Text-to-Speech (ElevenLabs)

`ElevenLabs.StreamingTTS` uses the ElevenLabs WebSocket streaming API.
The protocol:

1. **Connect** to `wss://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream-input`
2. **BOS** (Beginning of Stream): send `{"text": " "}` to initialize
3. **Text**: send `{"text": "Hello!", "flush": true}` for each sentence
4. **EOS** (End of Stream): send `{"text": ""}` when the LLM response is complete

Audio chunks arrive as base64-encoded PCM in `{"audio": "..."}` messages
and are decoded into `TTSAudioRawFrame`s. When `{"isFinal": true}` arrives,
a `TTSStoppedFrame` is pushed.

See `lib/feline/services/elevenlabs/streaming_tts.ex`.

### 8. Audio Playback

`AudioPlayer` buffers all `TTSAudioRawFrame` audio bytes for the current
utterance. When `TTSStoppedFrame` arrives, it writes the buffer to a temp
file and spawns `sox play` to play it back.

**Echo suppression coordination**: AudioPlayer pushes
`BotStartedSpeakingFrame` upstream when audio starts arriving, and
`BotStoppedSpeakingFrame` when the `play` process finishes (not when
buffering stops). This keeps the VAD's mic-mute active for the entire
duration of audible playback.

See `lib/feline/processors/audio_player.ex`.

## Interruption Flow

When you speak while the bot is talking:

1. VAD detects speech during `bot_speaking` and emits `InterruptionFrame`
2. `InterruptionFrame` is a system frame — it jumps the queue via selective receive
3. Each processor handles it: StreamingTTS closes the WebSocket, AudioPlayer
   kills the `play` process and discards buffered audio, SentenceAggregator
   clears its buffer
4. The new user speech flows through normally once the interruption clears

## Typed Input

You can also type messages directly in the console. The stdin reader thread
appends the message to the shared context and injects an `LLMContextFrame`
into the pipeline, bypassing STT entirely.

## Debug Logging

All internal TTS and WebSocket logging uses `Logger.debug`. Enable it with:

```bash
FELINE_DEBUG=1 mix feline.talk
```
