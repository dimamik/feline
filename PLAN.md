# Feline: Pipecat for Elixir — Implementation Plan

## Context

Pipecat is a Python framework for building real-time voice AI pipelines. It connects AI services (LLM, STT, TTS) with media transports (WebSocket, WebRTC) through a chain of frame processors. Frames are the universal data unit — audio, text, control signals — flowing bidirectionally through the pipeline.

Feline reimplements this in Elixir, replacing Python's asyncio with BEAM processes, queues with message passing, isinstance() with pattern matching, and manual task management with OTP supervision.

---

## Status

### Done

- **Phase 1** — Project scaffolding (`mix new`, `.tool-versions`, deps)
- **Phase 2** — Frame system (`Feline.Frame` macro, 29 frame types across system/data/control)
- **Phase 3** — Processor core (`Feline.Processor` behaviour, `Feline.Processor.Server` GenServer with priority via selective receive)
- **Phase 4** — Pipeline (`Feline.Pipeline` struct, Source/Sink processors, `Pipeline.Task` orchestrator, `Pipeline.Runner`)
- **Phase 5** — Service behaviours (`Feline.Services.LLM/STT/TTS` macros) + concrete implementations (OpenAI LLM, Deepgram STT, ElevenLabs TTS)
- **Phase 7** — Supporting modules (`Feline.Clock`, `Feline.Observer`, `Feline.Context`, `UserContextAggregator`, `AssistantContextAggregator`)
- **Phase 8** — Streaming services (OpenAI StreamingLLM with SSE, Deepgram StreamingSTT via WebSocket, ElevenLabs StreamingTTS via WebSocket)
- **Phase 10** — VAD (energy-based analyzer + `VADProcessor` with state machine)
- **Phase 11** — Image/video frames (`InputImageRawFrame`, `OutputImageRawFrame`, `SpriteFrame`, `VisionTextFrame`, `UserImageRawFrame`)
- **Phase 12** — Function calling (`FunctionCallHandler` processor)
- **Phase 13** — Audio utilities (`Feline.Audio.Utils` — RMS, silence detection, duration)
- **Phase 14** — Telemetry wired into `Processor.Server` (`frame_processed`, `frame_pushed` events)
- **Phase 15** — `Pipeline.Parallel` with fan-out and collector
- **Bug fixes** — Frame recategorization (audio/speaking/image frames → `:data`), shared context via `ContextAggregatorPair`, `Task.Supervisor` in StreamingLLM, safe `Base.decode64`, processor crash handling in `Pipeline.Task`, `try/rescue` in `dispatch_to_user`, `EndFrame` handling in streaming services, `WebSockex.send_frame` error handling, O(1) `Context.append_message`, cached `function_exported?`, unified `handle_frame/4`
- **Tests** — 36 tests covering frames, processors, pipelines, VAD, and function calling

### Not Yet Implemented

- **WebSocket transport** — actual WebSocket server/client using Bandit or Phoenix (current transports are base processor shells)
- **WebRTC transport** — Daily/LiveKit equivalents
- **Interruption completion handshake** — `ref` + caller PID on `InterruptionFrame`, Sink sends `{:interruption_complete, ref}` back
- **Output transport audio chunking** — fixed-size timed packets with bot speaking detection
- **Sentence aggregation** — buffer LLM tokens into complete sentences before TTS (current `sentence_end?` is naive)
- **Turn management strategies** — pluggable start/stop/mute strategies (currently hardcoded VAD only)
- **Registry-based processor linking** — for crash-restart resilience instead of captured PIDs
- **Backpressure** — no mechanism to prevent mailbox overflow on slow processors
- **User idle detection** — "Are you still there?" prompts
- **Audio resampling** — `Feline.Audio.Resampler` behaviour + linear interpolation
- **Metrics processor** — TTFB tracking, `MetricsFrame` reporting
- **Context summarization** — condensing long conversation contexts to stay within token limits
- **Serializers** — protobuf frame serialization for wire transport
- **Additional AI providers** — Anthropic, Google, Azure, Cartesia, etc.
- **Service switcher** — runtime provider swap/failover

---

## Phase 1: Project Scaffolding

### 1.1 Create project
```bash
mix new feline --sup
```

### 1.2 `.tool-versions`
```
erlang 27.3.2
elixir 1.18.3-otp-27
```

### 1.3 mix.exs dependencies
```elixir
{:jason, "~> 1.4"},
{:telemetry, "~> 1.2"},
{:req, "~> 0.5"},          # HTTP client for AI service APIs
{:websockex, "~> 0.4"}     # WebSocket client for streaming services (Deepgram STT)
```

---

## Phase 2: Frame System

### 2.1 `Feline.Frame` — macro for defining frame types

Each frame is an Elixir struct. A `use Feline.Frame` macro adds:
- Auto-generated `id` via `make_ref()`
- Module functions `__frame_category__/0` and `__frame_uninterruptible__/0`
- Common fields: `id`, `pts`, `metadata`

```elixir
# Usage:
defmodule Feline.Frames.TextFrame do
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :metadata, :text]
end
```

Helper functions on `Feline.Frame`:
- `system?(frame)` — checks `frame.__struct__.__frame_category__() == :system`
- `uninterruptible?(frame)` — checks `__frame_uninterruptible__/0`

### 2.2 Frame modules to create

**System frames** (`lib/feline/frames/system.ex`):
- `StartFrame` — fields: `clock`, `observer`
- `CancelFrame`
- `ErrorFrame` — fields: `message`, `exception`, `fatal`
- `InterruptionFrame`
- `MetricsFrame` — fields: `data`

**Data frames** (`lib/feline/frames/data.ex`):
- `TextFrame` — field: `text`
- `LLMTextFrame` — field: `text`
- `OutputAudioRawFrame` — fields: `audio`, `sample_rate`, `num_channels`
- `TTSAudioRawFrame` — fields: `audio`, `sample_rate`, `num_channels`
- `TranscriptionFrame` — fields: `text`, `language`
- `InterimTranscriptionFrame` — fields: `text`, `language`
- `LLMRunFrame`
- `LLMContextFrame` — field: `context`
- `LLMMessagesAppendFrame` — field: `messages`
- `FunctionCallInProgressFrame` — fields: `function_name`, `tool_call_id`, `arguments`
- `FunctionCallResultFrame` (also uninterruptible) — fields: `function_name`, `tool_call_id`, `result`
- `InputAudioRawFrame` — fields: `audio`, `sample_rate`, `num_channels`
- `UserStartedSpeakingFrame`, `UserStoppedSpeakingFrame`
- `BotStartedSpeakingFrame`, `BotStoppedSpeakingFrame`
- `InputImageRawFrame` — fields: `image`, `width`, `height`, `format`
- `UserImageRawFrame` — fields: `image`, `width`, `height`, `format`, `user_id`
- `OutputImageRawFrame` — fields: `image`, `width`, `height`, `format`
- `SpriteFrame` — field: `images`
- `VisionTextFrame` — fields: `text`, `image`, `width`, `height`, `format`

**Control frames** (`lib/feline/frames/control.ex`):
- `EndFrame` (uninterruptible)
- `StopFrame` (uninterruptible)
- `HeartbeatFrame`
- `LLMFullResponseStartFrame`, `LLMFullResponseEndFrame`
- `TTSStartedFrame`, `TTSStoppedFrame`

---

## Phase 3: Processor (the core)

### 3.1 `Feline.Processor` — behaviour

```elixir
@callback init(opts :: keyword()) :: {:ok, state}
@callback handle_frame(frame, direction, push_fn, state) ::
  {:ok, state} |
  {:push, frame, direction, state} |
  {:push_many, [{frame, direction}], state}
# Optional:
@callback handle_info(msg, push_fn, state) :: {:ok, state}
@callback handle_setup(setup, state) :: {:ok, state}
@callback handle_cleanup(state) :: :ok
```

The `push_fn` argument allows processors to push frames asynchronously (e.g., during streaming API calls). All processors receive it — those that don't need it can ignore it with `_push_fn`.

### 3.2 `Feline.Processor.Server` — GenServer implementation

Each processor is a GenServer. Key design:

**Message tags for priority**:
- `{:system_frame, frame, direction}` — processed immediately
- `{:frame, frame, direction}` — buffered when paused, drained after system frames

**Priority via selective receive**: Before processing each regular frame, do a non-blocking `receive` to drain any pending `:system_frame` messages. This gives system frames effective priority without a separate priority queue.

**State fields**:
- `mod` — the callback module
- `user_state` — module's custom state
- `next` / `prev` — PIDs of linked processors
- `paused` — boolean, buffers frames when true
- `cancelling` — boolean, drops non-system frames
- `buffered` — `:queue.new()` for paused frames
- `started` — boolean
- `has_handle_info_3` — cached `function_exported?` check (avoids per-frame introspection)

Frame dispatch is wrapped in `try/rescue` — exceptions produce an `ErrorFrame` pushed upstream instead of crashing the GenServer.

**Public API**:
- `Feline.Processor.queue_frame(pid, frame, direction)` — sends tagged message
- `Feline.Processor.link(a, b)` — sets a's next=b, b's prev=a

**Frame routing**:
- `push_frame(frame, :downstream, state)` → `send(state.next, {:tag, frame, :downstream})`
- `push_frame(frame, :upstream, state)` → `send(state.prev, {:tag, frame, :upstream})`

**Built-in frame handling** (before dispatching to user module):
- `StartFrame` → set `started: true`
- `CancelFrame` → set `cancelling: true`
- `InterruptionFrame` → clear buffered queue
- `EndFrame` → cleanup

---

## Phase 4: Pipeline

### 4.1 `Feline.Pipeline`

A struct holding processor specs. Not a process itself.

```elixir
defstruct [:processor_specs]

def new(specs) when is_list(specs)
```

Each spec is `{Module, opts_keyword_list}`.

### 4.2 `Feline.Pipeline.Task` — GenServer orchestrator

Starts processors under a `DynamicSupervisor`, links them, sends `StartFrame`, manages lifecycle.

**Responsibilities**:
- Start all processors as supervised children
- Link them in order (first processor's `next` = second, etc.)
- Call `handle_setup` on each
- Send `StartFrame` to the first processor
- Provide `queue_frames/2` API for external frame injection
- Handle `EndFrame` arriving at the sink (pipeline done)
- Heartbeat timer (optional)
- Idle timeout (optional)

The **sink processor** is a special built-in processor that, when it receives a downstream frame, sends it back to the `Pipeline.Task` process. This lets the task know when frames reach the end. Similarly, the **source processor** forwards upstream frames back to the task.

### 4.3 `Feline.Pipeline.Runner`

Simple module that starts a `Pipeline.Task`, traps exits, waits for completion.

```elixir
def run(pipeline, opts \\ [])
```

---

## Phase 5: Services

### 5.1 `Feline.Services.LLM` — behaviour

```elixir
@callback process_context(context, state) :: {:ok, output_frames, state}
```

The `use Feline.Services.LLM` macro defines a processor that:
- On `LLMContextFrame` → calls `process_context/2`, pushes resulting frames
- On `LLMRunFrame` → triggers LLM with current context
- Passes everything else through

### 5.2 `Feline.Services.STT` — behaviour

```elixir
@callback run_stt(audio_chunk, state) :: {:ok, frames, state} | {:continue, state}
```

Handles `InputAudioRawFrame`, calls `run_stt/2`, pushes `TranscriptionFrame` downstream.

### 5.3 `Feline.Services.TTS` — behaviour

```elixir
@callback run_tts(text, state) :: {:ok, audio_frames, state}
```

Handles `TextFrame`/`LLMTextFrame`, calls `run_tts/2`, pushes `TTSAudioRawFrame` downstream.

### 5.4 Concrete implementations (one each, for validation)

- `Feline.Services.OpenAI.LLM` — uses OpenAI chat completions API via `Req`
- `Feline.Services.Deepgram.STT` — WebSocket streaming to Deepgram
- `Feline.Services.ElevenLabs.TTS` — REST/WebSocket to ElevenLabs

---

## Phase 6: Transport

### 6.1 `Feline.Transport` — behaviour

```elixir
@callback input_spec(opts) :: {module(), keyword()}
@callback output_spec(opts) :: {module(), keyword()}
```

### 6.2 `Feline.Transports.WebSocket`

Uses a Phoenix Channel or raw WebSocket (via `WebSockex` or `Bandit`).

- **Input processor**: Receives binary audio from WebSocket, wraps as `InputAudioRawFrame`, pushes downstream
- **Output processor**: Receives `OutputAudioRawFrame`, sends binary audio over WebSocket

---

## Phase 7: Supporting Modules

### 7.1 `Feline.Clock`
```elixir
def monotonic_time_ns(), do: System.monotonic_time(:nanosecond)
```

### 7.2 `Feline.Observer` — behaviour
```elixir
@callback on_push_frame(data) :: :ok
@callback on_process_frame(data) :: :ok
```

Notifications sent via `:telemetry.execute/3`.

### 7.3 `Feline.Context` — LLM conversation context
A struct holding messages list, tools, tool_choice. Used by context aggregators.

### 7.4 `Feline.Processors.ContextAggregator`
- `UserContextAggregator` — accumulates user transcriptions into context, pushes `LLMRunFrame` on turn end
- `AssistantContextAggregator` — accumulates LLM responses into context

---

## Project Structure

```
feline/
├── .tool-versions
├── mix.exs
├── PLAN.md
├── lib/
│   ├── feline.ex                              # Top-level module, application
│   └── feline/
│       ├── frame.ex                           # use Feline.Frame macro + helpers
│       ├── frames/
│       │   ├── system.ex                      # System frame structs
│       │   ├── data.ex                        # Data frame structs
│       │   └── control.ex                     # Control frame structs
│       ├── processor.ex                       # Behaviour definition
│       ├── processor/
│       │   └── server.ex                      # GenServer implementation
│       ├── pipeline.ex                        # Pipeline struct
│       ├── pipeline/
│       │   ├── source.ex                      # Built-in source processor
│       │   ├── sink.ex                        # Built-in sink processor
│       │   ├── task.ex                        # Pipeline orchestrator GenServer
│       │   └── runner.ex                      # Top-level runner
│       ├── services/
│       │   ├── llm.ex                         # LLM behaviour + macro
│       │   ├── stt.ex                         # STT behaviour + macro
│       │   └── tts.ex                         # TTS behaviour + macro
│       ├── transport.ex                       # Transport behaviour
│       ├── transports/
│       │   ├── input.ex                       # Base input transport processor
│       │   └── output.ex                      # Base output transport processor
│       ├── clock.ex                           # Monotonic clock
│       ├── observer.ex                        # Observer behaviour
│       └── context.ex                         # LLM context struct
└── test/
    ├── test_helper.exs
    └── feline/
        ├── frame_test.exs
        ├── processor_test.exs
        └── pipeline_test.exs
```

---

## Key Design Decisions: Python → Elixir Mapping

| Python Pipecat | Feline (Elixir) | Why |
|---|---|---|
| `asyncio.PriorityQueue` | Selective receive with message tags | BEAM mailbox + selective receive is the native primitive |
| `isinstance()` dispatch | Pattern matching on structs | Idiomatic, compiler-checked |
| `prev`/`next` pointers | PIDs in GenServer state | Processes are the unit of composition |
| `asyncio.Task` | `Task.async` or spawned process | OTP handles lifecycle |
| Decorator event handlers | `:telemetry` events | Standard BEAM observability |
| Manual task cleanup | OTP supervision (`DynamicSupervisor`) | Automatic restart on crash |
| Single-threaded concurrency | True parallel processes | Each processor runs on its own scheduler |
| `try/except` error propagation | `ErrorFrame` upstream + supervisor restart | Both app-level and infrastructure-level recovery |

---

## Verification Plan

1. **Unit tests**: Frame creation, category checks, processor init/frame handling
2. **Integration test**: Build a 3-processor pipeline (source → passthrough → sink), send frames through, verify they arrive at the sink
3. **Priority test**: Send mixed system/data frames, verify system frames processed first
4. **Interruption test**: Send data frames, then InterruptionFrame, verify buffered data frames are dropped
5. **End-to-end**: Build a mock pipeline with fake STT → LLM → TTS processors, verify frame flow from audio input to audio output

---

## Implementation Order (Completed)

1. ~~`mix new feline --sup` + `.tool-versions` + `mix.exs`~~
2. ~~`Feline.Frame` macro~~
3. ~~All frame structs (~29 types)~~
4. ~~`Feline.Processor` behaviour + `Feline.Processor.Server` GenServer~~
5. ~~`Feline.Pipeline` struct + `Feline.Pipeline.Source` + `Feline.Pipeline.Sink`~~
6. ~~`Feline.Pipeline.Task` + `Feline.Pipeline.Runner`~~
7. ~~Tests for core pipeline~~
8. ~~`Feline.Services.LLM/STT/TTS` behaviours~~
9. ~~`Feline.Context` + aggregators~~
10. ~~`Feline.Transport` behaviour~~
11. ~~Concrete service implementations (OpenAI LLM, Deepgram STT, ElevenLabs TTS)~~

---

## Phase 8: Streaming Services (Done)

Streaming services push frames asynchronously via the `push_fn` passed to `handle_frame/4`.

### 8.2 Streaming OpenAI LLM — `Feline.Services.OpenAI.StreamingLLM`

- Uses `Req.post` with `into: :self` for SSE streaming
- Runs streaming in a `Task.Supervisor.async_nolink` task (supervised, with error propagation)
- Parses SSE `data: {...}` lines, pushes `LLMTextFrame` per content delta
- Accumulates `tool_calls` arguments across deltas, flushes as `FunctionCallInProgressFrame`
- On task crash: sends `LLMFullResponseEndFrame` so downstream never hangs
- `InterruptionFrame` terminates the in-flight task via `Task.Supervisor.terminate_child`

### 8.3 Streaming Deepgram STT — `Feline.Services.Deepgram.StreamingSTT`

- Persistent WebSocket via `WebSockex` to `wss://api.deepgram.com/v1/listen`
- `InputAudioRawFrame` → binary audio sent to WebSocket (with error handling on send)
- WebSocket receives JSON → `{:deepgram_transcript, result}` message to processor
- Processor pushes `TranscriptionFrame` / `InterimTranscriptionFrame` via `push_fn`
- `EndFrame` closes the WebSocket connection cleanly

### 8.4 Streaming ElevenLabs TTS — `Feline.Services.ElevenLabs.StreamingTTS`

- WebSocket streaming to ElevenLabs voice synthesis endpoint
- Buffers text until sentence boundary, then sends to WebSocket
- WebSocket receives base64-encoded audio → decoded safely with `Base.decode64/1` → pushed as `TTSAudioRawFrame`
- `EndFrame` flushes remaining text and closes WebSocket
- `InterruptionFrame` flushes and resets state

---

## Phase 9: WebSocket Transport

### 9.1 `Feline.Transports.WebSocket.Server`

Server-side WebSocket using Bandit + WebSock. Runs as a separate supervised process.

**Input flow**:
1. Client connects via WebSocket
2. Binary messages → `InputAudioRawFrame` → queued into the first pipeline processor
3. Text messages → `InputTransportMessageFrame` (JSON payload)

**Output flow**:
1. Pipeline produces `OutputAudioRawFrame` / `TTSAudioRawFrame`
2. Output processor sends binary audio over WebSocket
3. `OutputTransportMessageFrame` → sent as text

### 9.2 Audio Output Chunking

Pipecat chunks output audio into 10ms × N segments (default N=4 → 40ms):

```elixir
chunk_bytes = div(sample_rate, 100) * num_channels * 2 * chunks_per_send
```

The output transport buffers audio and sends fixed-size packets at real-time intervals using `Process.send_after/3` for timing simulation.

### 9.3 Transport Params

```elixir
defstruct [
  audio_in_enabled: true,
  audio_in_sample_rate: 16_000,
  audio_out_enabled: true,
  audio_out_sample_rate: 24_000,
  audio_out_10ms_chunks: 4,
  audio_out_end_silence_secs: 2.0,
  session_timeout_secs: nil
]
```

### 9.4 New dependency

```elixir
{:bandit, "~> 1.6"},
{:websock_adapter, "~> 0.5"}
```

---

## Phase 10: VAD (Done)

Energy-based VAD implemented in `Feline.Audio.VAD.Energy` with `Feline.Processors.VADProcessor` state machine (`QUIET ↔ SPEAKING`). Configurable `start_secs`/`stop_secs` debounce. Emits `UserStartedSpeakingFrame`/`UserStoppedSpeakingFrame`. Triggers `InterruptionFrame` when user speaks while bot is speaking.

---

## Phase 11: Image/Video Frames (Done)

All image/video frame types are defined in `lib/feline/frames/data.ex`:
`InputImageRawFrame`, `OutputImageRawFrame`, `UserImageRawFrame`, `SpriteFrame`, `VisionTextFrame` — all categorized as `:data`.

---

## Phase 12: Function Calling (Done)

`Feline.Processors.FunctionCallHandler` intercepts `FunctionCallInProgressFrame`, looks up handler from a `functions` map, executes it, and pushes `FunctionCallResultFrame`. Unknown functions pass through.

---

## Phase 13: Audio Utilities (Done)

`Feline.Audio.Utils` — pure Elixir binary operations on PCM16: `silence?/2`, `mix_audio/2`, `compute_rms/1`, `generate_silence/2`.

Audio resampling (`Feline.Audio.Resampler` behaviour) is listed as future work.

---

## Phase 14: Observer & Telemetry Integration (Done)

Telemetry wired into `Processor.Server` — emits `[:feline, :processor, :frame_processed]` and `[:feline, :processor, :frame_pushed]` events.

Metrics processor (TTFB tracking, `MetricsFrame` reporting) is listed as future work.

---

## Phase 15: ParallelPipeline (Done)

`Feline.Pipeline.Parallel` — fan-out coordinator that broadcasts system frames to all branches and routes data frames (round-robin or by type). Collector merges branch outputs into single downstream.

---

## Remaining Implementation Order

All core phases (1–8, 10–15) are complete. Remaining work:

1. **Phase 9**: WebSocket transport with Bandit (actual server/client, audio chunking, transport params)
2. **Interruption completion handshake** — `ref` + caller PID on `InterruptionFrame`, Sink acks back
3. **Sentence aggregation** — buffer LLM tokens into complete sentences before TTS
4. **Turn management strategies** — pluggable start/stop/mute beyond hardcoded VAD
5. **Registry-based processor linking** — crash-restart resilience
6. **Backpressure** — mailbox overflow prevention on slow processors
7. **Audio resampling** — `Feline.Audio.Resampler` behaviour + implementation
8. **Metrics processor** — TTFB tracking, `MetricsFrame` reporting
9. **Additional AI providers** — Anthropic, Google, Azure, Cartesia, etc.
10. **Serializers** — protobuf frame serialization for wire transport
