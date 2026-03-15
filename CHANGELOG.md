# Changelog

All notable changes to Feline are documented here. Feline is pre-1.0 —
all releases are release candidates targeting 0.1.0.

## 0.1.0-rc.2 (2026-03-15)

### Added

- **RTVI protocol support** — implements the [Real-Time Voice Interaction](https://docs.pipecat.ai/client/introduction) protocol (v1.2.0), enabling compatibility with pipecat-client-web and other RTVI clients
  - `Feline.RTVI.Messages` — pure functions for building RTVI JSON message envelopes
  - `Feline.RTVI.Processor` — pipeline processor that handles inbound RTVI messages (`client-ready`, `send-text`, `disconnect-bot`, `llm-function-call-result`)
  - `Feline.RTVI.EventEmitter` — pipeline processor that converts outbound frames (transcriptions, LLM tokens, TTS events) to RTVI JSON messages
- **Phoenix LiveView integration** (optional dependency)
  - `Feline.Phoenix.VoiceBot` — LiveView function component (`<.voice_bot>`) that renders a hook-enabled container for the JS client
  - `Feline.Phoenix.RTVIHandler` — WebSock handler that starts a pipeline per WebSocket connection, replacing the need to manually manage server/pipeline lifecycle
  - `Feline.Phoenix.RTVIPlug` — Plug for mounting the RTVI WebSocket in a Phoenix router via `forward`
- **JavaScript client assets** (`priv/static/feline/`) — importable in Phoenix apps with a JS build step
  - `transport.js` — `FelineWebSocketTransport`, a custom Transport for @pipecat-ai/client-js that handles mic capture (48kHz→16kHz PCM via AudioWorklet), audio playback (24kHz PCM→48kHz via AudioWorklet), and RTVI messaging over a single WebSocket
  - `hook.js` — `FelineVoiceBot` LiveView hook that bridges the audio WebSocket and LiveView channel
- `mix feline.dev` — browser-based voice assistant demo using Phoenix LiveView + RTVI over plain WebSocket. This is an alternative to `mix feline.talk_webrtc` (Boombox WebRTC) that requires no native dependencies and integrates with the standard Phoenix stack
- Guide: [Phoenix Voice Bot](guides/phoenix-voice-bot.md) — step-by-step integration guide

### Changed

- `Feline.Transports.WebSocket.Output` — added `rtvi_enabled` option to emit `bot-started-speaking` / `bot-stopped-speaking` RTVI messages when the bot starts/stops playing audio
- `Feline.Phoenix.RTVIPlug` — path matching uses `conn.path_info` instead of `conn.request_path` to work correctly with Phoenix `forward`

### Dependencies

- Added optional: `phoenix ~> 1.7`, `phoenix_live_view ~> 1.0`, `phoenix_html ~> 4.0`

## 0.1.0-rc.1 (2026-02-27)

Initial experimental release.

### Added

- Frame system — 40+ frame types across system/data/control categories
- `Feline.Processor` behaviour and `Processor.Server` GenServer with selective-receive priority
- `Feline.Pipeline` struct, `Pipeline.Task` orchestrator, `Pipeline.Runner`
- `Pipeline.Parallel` for fan-out to concurrent processor branches
- Service behaviours: `Feline.Services.LLM`, `Feline.Services.STT`, `Feline.Services.TTS`
- OpenAI chat completions (batch + SSE streaming)
- Deepgram STT (REST + WebSocket streaming)
- ElevenLabs TTS (REST + WebSocket streaming)
- `Feline.Processors.SentenceAggregator` — buffers LLM token stream into sentences
- `Feline.Processors.FunctionCallHandler` — executes LLM tool calls
- `Feline.Processors.VADProcessor` — energy-based voice activity detection
- `UserContextAggregator` / `AssistantContextAggregator` for LLM conversation context
- WebSocket transport via Bandit (`Feline.Transports.WebSocket`)
- `Feline.Audio.Utils` — PCM16 utilities (RMS, silence detection, mixing)
- Telemetry events for frame processing metrics
