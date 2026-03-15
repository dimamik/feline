# Phoenix Voice Bot — Integration Guide

Add a real-time voice AI assistant to your Phoenix LiveView app using
Feline's `VoiceBot` component. Audio flows over a raw WebSocket (PCM),
while the LiveView channel handles UI updates via RTVI protocol events.

## Prerequisites

- A Phoenix 1.7+ app with LiveView
- API keys for your chosen services (e.g. Deepgram, OpenAI, ElevenLabs)

## 1. Add Dependencies

```elixir
# mix.exs
defp deps do
  [
    {:feline, "~> 0.1"},
    {:phoenix, "~> 1.7"},
    {:phoenix_live_view, "~> 1.0"}
  ]
end
```

Run `mix deps.get`.

## 2. Define a Pipeline Builder

Create a module that builds a Feline pipeline given a WebSocket PID.
The `ws_pid` is the handler process that sends audio/messages back to
the browser.

```elixir
# lib/my_app/voice_pipeline.ex
defmodule MyApp.VoicePipeline do
  def build(ws_pid) do
    context =
      Feline.Context.new([
        %{"role" => "system", "content" => "You are a helpful voice assistant."}
      ])

    {:ok, pair} = Feline.Processors.ContextAggregatorPair.start(context)

    Feline.Pipeline.new([
      # Parses inbound RTVI messages (client-ready, send-text, etc.)
      {Feline.RTVI.Processor, []},

      # Voice activity detection
      {Feline.Processors.VADProcessor, start_secs: 0.2, stop_secs: 0.8},

      # Speech-to-text
      {Feline.Services.Deepgram.StreamingSTT,
       api_key: System.fetch_env!("DEEPGRAM_API_KEY"), sample_rate: 16_000},

      # Context management
      {Feline.Processors.UserContextAggregator, context_agent: pair.agent},
      {Feline.Services.OpenAI.StreamingLLM,
       api_key: System.fetch_env!("OPENAI_API_KEY"), model: "gpt-4.1-mini"},
      {Feline.Processors.AssistantContextAggregator, context_agent: pair.agent},

      # Sentence buffering
      {Feline.Processors.SentenceAggregator, []},

      # Converts pipeline frames to RTVI JSON messages (transcripts, LLM tokens, etc.)
      {Feline.RTVI.EventEmitter, []},

      # Text-to-speech
      {Feline.Services.Deepgram.StreamingTTS,
       api_key: System.fetch_env!("DEEPGRAM_API_KEY"), sample_rate: 24_000},

      # Sends audio + RTVI messages back to browser over WebSocket
      {Feline.Transports.WebSocket.Output,
       ws_pid: ws_pid, rtvi_enabled: true,
       params: %Feline.TransportParams{audio_out_sample_rate: 24_000}}
    ])
  end
end
```

## 3. Mount the WebSocket Route

The RTVI WebSocket runs **separately** from LiveView's channel — it
carries binary PCM audio and JSON messages directly.

```elixir
# lib/my_app_web/router.ex
forward "/feline", Feline.Phoenix.RTVIPlug,
  pipeline_builder: &MyApp.VoicePipeline.build/1
```

This upgrades `GET /feline/ws` to a WebSocket handled by
`Feline.Phoenix.RTVIHandler`, which starts a fresh pipeline per
connection.

## 4. Create the LiveView

```elixir
# lib/my_app_web/live/voice_live.ex
defmodule MyAppWeb.VoiceLive do
  use MyAppWeb, :live_view
  import Feline.Phoenix.VoiceBot

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, status: :idle, messages: [], bot_text_acc: "")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <.voice_bot id="voice-bot" ws_url="/feline/ws">
      <%= if @status == :idle do %>
        <button phx-click="start">Start</button>
      <% else %>
        <button phx-click="stop">Stop</button>
      <% end %>
    </.voice_bot>

    <div :for={msg <- @messages}>
      <strong><%= msg.role %>:</strong> <%= msg.text %>
    </div>
    """
  end

  # Start/stop voice session
  @impl true
  def handle_event("start", _params, socket) do
    {:noreply, socket |> assign(status: :connecting) |> push_event("feline:connect", %{})}
  end

  def handle_event("stop", _params, socket) do
    {:noreply, socket |> assign(status: :idle, bot_text_acc: "") |> push_event("feline:disconnect", %{})}
  end

  # Events pushed from the JS hook
  def handle_event("feline:connected", _params, socket) do
    {:noreply, assign(socket, status: :connected)}
  end

  def handle_event("feline:disconnected", _params, socket) do
    {:noreply, assign(socket, status: :idle, bot_text_acc: "")}
  end

  def handle_event("feline:error", %{"message" => msg}, socket) do
    {:noreply, socket |> assign(status: :idle) |> update(:messages, &(&1 ++ [%{role: "system", text: msg}]))}
  end

  def handle_event("feline:rtvi", %{"type" => type} = msg, socket) do
    handle_rtvi(type, msg["data"] || %{}, socket)
  end

  def handle_event(_event, _params, socket), do: {:noreply, socket}

  # RTVI message handlers
  defp handle_rtvi("bot-ready", _data, socket) do
    {:noreply, assign(socket, status: :ready)}
  end

  defp handle_rtvi("user-transcription", %{"text" => text, "final" => true}, socket) do
    {:noreply, update(socket, :messages, &(&1 ++ [%{role: "user", text: text}]))}
  end

  defp handle_rtvi("bot-llm-text", %{"text" => text}, socket) do
    {:noreply, assign(socket, bot_text_acc: socket.assigns.bot_text_acc <> text)}
  end

  defp handle_rtvi("bot-llm-stopped", _data, socket) do
    if socket.assigns.bot_text_acc != "" do
      {:noreply, socket |> update(:messages, &(&1 ++ [%{role: "bot", text: socket.assigns.bot_text_acc}])) |> assign(bot_text_acc: "")}
    else
      {:noreply, socket}
    end
  end

  defp handle_rtvi(_type, _data, socket), do: {:noreply, socket}
end
```

## 5. Add the JS Hook

The `<.voice_bot>` component attaches a `phx-hook="FelineVoiceBot"` to
its container div. You need to register this hook with your LiveSocket.

### Option A: Use the bundled hook (with a JS build step)

```javascript
// assets/js/app.js
import { FelineVoiceBot } from "../../deps/feline/priv/static/feline/hook.js"

let liveSocket = new LiveSocket("/live", Socket, {
  hooks: { FelineVoiceBot }
})
```

### Option B: Inline the hook (no build step)

Add this `<script>` tag **before** the LiveSocket boot script in your
root layout. See `mix feline.dev` source for a complete working example
(`lib/mix/tasks/feline.dev.ex`), which inlines the AudioWorklet
processors, the WebSocket transport, and the hook all in a single
`<script>` block.

The hook handles:
- Opening a raw WebSocket to `/feline/ws`
- Capturing mic audio via AudioWorklet (48kHz → 16kHz PCM)
- Playing back bot audio via AudioWorklet (24kHz PCM → 48kHz speakers)
- Forwarding RTVI JSON messages to LiveView via `pushEvent`

## 6. Add the LiveView Route

```elixir
# lib/my_app_web/router.ex
scope "/", MyAppWeb do
  pipe_through :browser

  live "/voice", VoiceLive
end
```

## Architecture

```
Browser                           Feline (Elixir)
┌─────────────┐                  ┌─────────────────────────────┐
│ LiveView     │ ◄── LV channel ──► VoiceLive (handle_event)   │
│ (UI updates) │                  │   status, transcripts, etc  │
└──────┬───────┘                  └─────────────────────────────┘
       │
       │ pushEvent("feline:rtvi", msg)
       │
┌──────┴───────┐                  ┌─────────────────────────────┐
│ JS Hook      │                  │ RTVIHandler (WebSock)        │
│ FelineVoice  │ ◄── WebSocket ──► │ ├─ RTVI.Processor           │
│ Bot          │   binary PCM +   │ ├─ VAD → STT → LLM → TTS   │
│              │   JSON RTVI      │ ├─ RTVI.EventEmitter         │
└──────────────┘                  │ └─ WebSocket.Output          │
                                  └─────────────────────────────┘
```

Two connections run in parallel:
1. **LiveView channel** (`/live`) — standard Phoenix LiveView WebSocket
   for UI state (button states, transcript display, etc.)
2. **RTVI WebSocket** (`/feline/ws`) — raw binary PCM audio + RTVI JSON
   messages for the voice pipeline

The JS hook bridges them: it receives RTVI messages from the audio
WebSocket and forwards them to the LiveView via `pushEvent`.

## RTVI Events Reference

Events pushed from the JS hook to your LiveView as
`handle_event("feline:rtvi", msg, socket)`:

| `msg["type"]`            | `msg["data"]`                              | When                          |
|--------------------------|--------------------------------------------|-------------------------------|
| `bot-ready`              | `%{"version" => "1.2.0"}`                  | Pipeline ready                |
| `user-transcription`     | `%{"text" => "...", "final" => true/false}` | User speech recognized        |
| `bot-llm-text`           | `%{"text" => "..."}`                       | Streamed LLM token            |
| `bot-llm-started`        | `%{}`                                      | LLM inference started         |
| `bot-llm-stopped`        | `%{}`                                      | LLM inference complete        |
| `bot-tts-text`           | `%{"text" => "..."}`                       | Sentence sent to TTS          |
| `bot-tts-started`        | `%{}`                                      | TTS synthesis started         |
| `bot-tts-stopped`        | `%{}`                                      | TTS synthesis complete        |
| `bot-started-speaking`   | `%{}`                                      | Audio playback started        |
| `bot-stopped-speaking`   | `%{}`                                      | Audio playback stopped        |
| `user-started-speaking`  | `%{}`                                      | VAD detected speech           |
| `user-stopped-speaking`  | `%{}`                                      | VAD detected silence          |

## Quick Test

Run the built-in demo to verify everything works:

```bash
# Set your API keys in .env
echo 'OPENAI_API_KEY=sk-...' >> .env
echo 'DEEPGRAM_API_KEY=...' >> .env

mix feline.dev
# Open http://localhost:4000
```
