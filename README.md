# Feline

A port of [pipecat](https://github.com/pipecat-ai/pipecat) (real-time voice AI pipelines) to Elixir/OTP.

Feline speaks the same wire protocol as a Python pipecat server, so unmodified pipecat web clients (`@pipecat-ai/client-js` + `@pipecat-ai/websocket-transport`) connect to it directly. Each processor runs in its own GenServer; slow work (HTTP, streaming) runs in monitored tasks, and interruptions kill in-flight work and flush queued frames.

## What's included

- **Pipeline** - `Feline.Pipeline.Task` runs a list of `Feline.Processor` modules, pushing frames downstream/upstream between them.
- **Transport** - `Feline.Transports.WebSocket` (Bandit) serving the pipecat protobuf protocol, plus an RTVI processor/reporter for client events.
- **Services** - Deepgram STT, OpenAI LLM (with function calling), ElevenLabs and Cartesia TTS.
- **Voice plumbing** - energy-threshold VAD, user turn detection, sentence and context aggregation.

## Quick start

Requires Elixir ~> 1.19 and API keys for Deepgram, OpenAI, and ElevenLabs.

```sh
cp .env.example .env   # fill in DEEPGRAM_API_KEY, OPENAI_API_KEY, ELEVENLABS_API_KEY
mix deps.get
mix run examples/voice_bot.exs
```

Then start the web client - see [examples/README.md](examples/README.md).

## Building a bot

A bot is a list of processors handed to a pipeline task, one per connection:

```elixir
bot = fn transport ->
  Feline.Pipeline.Task.start_link(
    processors: [
      Feline.Transports.WebSocket.Input.spec(transport),
      Feline.RTVI.Processor,
      Feline.Audio.EnergyVAD,
      Feline.Turns.UserTurnProcessor,
      Feline.Services.Deepgram.STT,
      {Feline.Processors.ContextAggregator, system_prompt: "You are a helpful voice assistant."},
      Feline.Services.OpenAI.LLM,
      Feline.Processors.SentenceAggregator,
      Feline.Services.ElevenLabs.TTS,
      Feline.Processors.AssistantCollector,
      Feline.RTVI.Reporter,
      Feline.Transports.WebSocket.Output.spec(transport)
    ]
  )
end

Feline.Transports.WebSocket.start_link(port: 7860, bot: bot)
```

Custom processors implement the `Feline.Processor` behaviour (`use Feline.Processor` gives pass-through defaults):

```elixir
defmodule MyProcessor do
  use Feline.Processor

  @impl true
  def handle_frame(%Feline.Frames.TextFrame{} = frame, direction, _ctx, state) do
    {:push, %{frame | text: String.upcase(frame.text)}, direction, state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
end
```

See [examples/voice_bot.exs](examples/voice_bot.exs) for the complete picture, including function calling.

## Tests

```sh
mix test
```
