# Feline

Real-time voice and multimodal AI pipelines for Elixir, inspired by [pipecat](https://github.com/pipecat-ai/pipecat).

> **Disclaimer:** Feline is an experiment in porting pipecat to Elixir using only LLMs (no human-written code). It is not reliable yet — expect rough edges, missing features, and untested paths. Use at your own risk.

Feline reimplements pipecat's core architecture using BEAM/OTP primitives — each processor is a GenServer, pipelines are supervised process trees, and frame priority is handled through selective receive rather than async queues.

## Core Concepts

**Frames** are the universal data unit. Audio, text, transcriptions, LLM responses, control signals — everything flows through the pipeline as a frame. Frames are categorized as:

- **System** — high priority, processed immediately (e.g. `StartFrame`, `CancelFrame`, `InterruptionFrame`)
- **Data** — regular content (e.g. `TextFrame`, `OutputAudioRawFrame`, `TranscriptionFrame`)
- **Control** — lifecycle signals (e.g. `EndFrame`, `HeartbeatFrame`, `TTSStartedFrame`)

**Processors** are GenServer processes that receive frames, transform them, and push them downstream (or upstream for errors). Each processor implements the `Feline.Processor` behaviour:

```elixir
defmodule MyApp.Uppercaser do
  use Feline.Processor

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_frame(%Feline.Frames.TextFrame{text: text} = frame, :downstream, state) do
    {:push, %{frame | text: String.upcase(text)}, :downstream, state}
  end

  def handle_frame(frame, direction, state) do
    {:push, frame, direction, state}
  end
end
```

**Pipelines** chain processors together. A `Pipeline.Task` starts all processors under a `DynamicSupervisor`, links them in order, and sends a `StartFrame` to kick things off:

```elixir
pipeline = Feline.Pipeline.new([
  {MyApp.STT, api_key: "..."},
  {MyApp.LLM, api_key: "..."},
  {MyApp.TTS, api_key: "..."}
])

Feline.Pipeline.Runner.run(pipeline)
```

**Services** are processors with pre-built frame handling for common AI tasks. Implement one callback and the service macro handles the rest:

- `Feline.Services.LLM` — receives `LLMContextFrame`, calls your `process_context/2`, pushes `LLMTextFrame`
- `Feline.Services.STT` — receives `InputAudioRawFrame`, calls your `run_stt/2`, pushes `TranscriptionFrame`
- `Feline.Services.TTS` — receives `TextFrame`, calls your `run_tts/2`, pushes `TTSAudioRawFrame`

## How It Works

```
[Source] → [Processor 1] → [Processor 2] → ... → [Sink]
   ↑ upstream                              downstream ↓
```

1. `Pipeline.Task` starts all processors as GenServer processes under a `DynamicSupervisor`
2. Processors are linked in order — each knows its `next` and `prev` PID
3. Frames flow via message passing: `send(next_pid, {:frame, frame, :downstream})`
4. System frames use a separate tag `{:system_frame, ...}` and are drained from the mailbox before each regular frame via selective receive
5. Interruptions clear buffered frames; cancellation drops all non-system frames
6. The sink forwards frames back to the `Pipeline.Task`, which manages lifecycle (EndFrame = done)

## Key Differences from Python Pipecat

| Python pipecat | Feline |
|---|---|
| `asyncio.PriorityQueue` | Selective receive on message tags |
| `isinstance()` dispatch | Pattern matching on structs |
| `prev`/`next` object pointers | PIDs in GenServer state |
| `asyncio.Task` management | OTP `DynamicSupervisor` |
| Single-threaded concurrency | True parallel BEAM processes |
| `try/except` error handling | `ErrorFrame` upstream + supervisor restart |

## Built-in Services

| Service | Module | Streaming |
|---|---|---|
| OpenAI Chat Completions | `Feline.Services.OpenAI.LLM` | `Feline.Services.OpenAI.StreamingLLM` |
| Deepgram STT | `Feline.Services.Deepgram.STT` | `Feline.Services.Deepgram.StreamingSTT` |
| ElevenLabs TTS | `Feline.Services.ElevenLabs.TTS` | `Feline.Services.ElevenLabs.StreamingTTS` |

## Additional Features

- **Parallel pipelines** — `Feline.Pipeline.Parallel` fans out frames to concurrent processor branches
- **Voice Activity Detection** — energy-based VAD processor with configurable thresholds
- **Function call handling** — `Feline.Processors.FunctionCallHandler` for LLM tool calls
- **Context aggregation** — `UserContextAggregator` and `AssistantContextAggregator` for managing LLM conversation state
- **Telemetry** — `:telemetry` hooks and observer callbacks for frame processing metrics

## Live Voice Demo

Talk to an AI agent through your microphone. Requires API keys and `sox` (`brew install sox`).

1. Add keys to `.env` in the project root:

```
OPENAI_API_KEY=sk-...
DEEPGRAM_API_KEY=...
ELEVENLABS_API_KEY=...
ELEVENLABS_VOICE_ID=...
```

2. Run:

```bash
mix feline.talk
```

Speak into your mic and the agent responds in real-time — both text and audio. You can also type messages directly in the console.

Customize the system prompt:

```bash
mix feline.talk --system "You are a pirate. Respond in pirate speak."
```

The demo pipeline ([source](lib/mix/tasks/feline.talk.ex)):

```
Mic (ffmpeg) → VAD → Deepgram STT → Context Aggregation → OpenAI LLM → Sentence Aggregation → ElevenLabs TTS → Speaker (sox)
```

Features working in the demo:
- Streaming speech-to-text and text-to-speech
- Streaming LLM token output (printed to console as it arrives)
- Echo suppression (mic is muted while bot speaks)
- User interruption (speak while bot is talking to cut it off)

## Installation

Add `feline` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:feline, "~> 0.1"}
  ]
end
```
