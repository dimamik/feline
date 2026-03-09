# WebRTC Voice Demo — In Depth

This guide walks through how `mix feline.talk_webrtc` works: how browser
audio flows over WebRTC through the pipeline, gets transcribed, generates
an LLM response, synthesizes speech, streams it back to the browser, and
displays live captions — all in real time.

For the simpler local-audio variant, see the [Live Voice Demo](live-voice-demo.md).

## The Pipeline

```
Browser Mic (WebRTC)
  │
  ▼  Boombox.read → InputAudioRawFrame (16kHz, mono, 16-bit PCM)
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
CaptionSender ─── sends each sentence to browser via text WebSocket
  │
  ▼  TextFrame
ElevenLabs.StreamingTTS ─── WebSocket streaming, emits audio chunks
  │
  ▼  TTSAudioRawFrame (24kHz, mono, 16-bit PCM)
AudioOutput ─── sends audio packets to Boombox writer → WebRTC → browser
  │
  ▼  BotStartedSpeakingFrame / BotStoppedSpeakingFrame (upstream)
```

## Architecture

The demo uses three WebSocket connections and two WebRTC peer connections
between the browser and the server:

```
Browser                          Server (Bandit)
───────                          ───────────────
  /ws/input  ◄──── signaling ────►  Boombox Reader (mic audio in)
  /ws/output ◄──── signaling ────►  Boombox Writer (bot audio out)
  /ws/text   ◄──── captions  ────►  CaptionSender
```

- **`/ws/input`** and **`/ws/output`** are WebRTC signaling channels that
  exchange SDP offers/answers and ICE candidates via `Membrane.WebRTC.Signaling`.
- **`/ws/text`** is a plain WebSocket for sending caption text to the browser.

The HTTP server (`Bandit` + `Boombox.Plug`) serves the HTML page and handles
all three WebSocket upgrades.

## Startup Sequence

1. Load environment variables (`.env` file)
2. Create two `Membrane.WebRTC.Signaling` processes (input + output)
3. Create a text registry Agent (tracks the `/ws/text` WebSocket PID)
4. Start `Bandit` HTTP server with the Plug
5. Start two Boombox servers in parallel (reader + writer) — these block
   until the browser connects and WebRTC negotiation completes
6. Build and start the Feline pipeline
7. Enter the reader loop: `Boombox.read` → chunk into frames → pipeline

## Stage by Stage

### 1. Browser Audio Capture (WebRTC → Boombox Reader)

The browser calls `getUserMedia({audio: true})` to get the microphone stream,
creates an `RTCPeerConnection`, and negotiates with the server via the
`/ws/input` signaling WebSocket.

On the server side, a Boombox reader receives the WebRTC audio and converts
it to raw PCM. The Mix task reads packets with `Boombox.read/1` in a loop,
chunks them into 640-byte frames (20ms at 16kHz), and injects
`InputAudioRawFrame`s into the pipeline.

### 2–7. VAD → STT → Context → LLM → Sentence Aggregation → TTS

These stages work identically to the local audio demo. See the
[Live Voice Demo guide](live-voice-demo.md) for details.

### 8. Live Captions (CaptionSender)

`CaptionSender` sits between `SentenceAggregator` and `StreamingTTS` in
the pipeline. When it receives a `TextFrame` (a complete sentence), it
sends the text directly to the browser via the `/ws/text` WebSocket:

```
SentenceAggregator emits TextFrame "Hello, how can I help?"
  ↓
CaptionSender sends {type: "caption", text: "Hello, how can I help?"} to browser
  ↓
TextFrame continues downstream to TTS
```

Captions appear as soon as each sentence is assembled — slightly before the
corresponding audio plays. The browser keeps the last 3 caption lines visible,
with the most recent at full opacity.

### 9. Audio Output (AudioOutput → Boombox Writer → WebRTC)

`AudioOutput` receives `TTSAudioRawFrame` chunks from the TTS processor and
sends them to the Boombox writer for delivery over WebRTC to the browser.

Each audio chunk becomes a `Boombox.Packet` with a monotonically increasing
presentation timestamp (`pts`), calculated from cumulative audio duration.

The Boombox writer runs in **async message mode** (`communication_medium:
:messages`). Audio packets are sent via `send(pid, {:boombox_packet, packet})`
rather than `Boombox.write/2` (which uses `GenServer.call` with a 5s default
timeout). This prevents the processor from blocking on Membrane's demand-based
back-pressure, which matters when TTS generates audio faster than real-time.

**Echo suppression**: AudioOutput pushes `BotStartedSpeakingFrame` upstream
when the first audio chunk arrives, and `BotStoppedSpeakingFrame` when
`TTSStoppedFrame` signals the end of the utterance. This keeps the VAD's
mic-mute active during playback.

## Key Differences from Local Audio Demo

| Aspect | `mix feline.talk` | `mix feline.talk_webrtc` |
|--------|-------------------|--------------------------|
| Audio input | ffmpeg + macOS mic | WebRTC from browser |
| Audio output | sox playback | WebRTC to browser |
| Captions | None | Live captions via WebSocket |
| Boombox writer mode | N/A | Async messages (no blocking) |
| Output sample rate | 24kHz | 24kHz |
| Input sample rate | 16kHz | 16kHz |

## Interruption Flow

Same as the local demo, with one addition: `CaptionSender` is transparent to
`InterruptionFrame` — it passes through without side effects. The TTS
WebSocket is closed, audio output stops, and the browser naturally stops
receiving audio.

## Typed Input

You can type messages in the console while the WebRTC demo is running. The
stdin reader appends the message to the shared context and injects an
`LLMContextFrame` into the pipeline, bypassing STT.
