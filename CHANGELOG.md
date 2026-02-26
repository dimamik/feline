# Changelog

## 0.1.0 (2026-02-27)

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
