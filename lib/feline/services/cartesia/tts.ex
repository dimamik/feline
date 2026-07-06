defmodule Feline.Services.Cartesia.TTS do
  @moduledoc """
  Streaming text-to-speech over Cartesia's websocket API.

  Sentences (`TextFrame`) synthesize one at a time - the next request is sent
  when the previous context reports done, preserving audio order. Audio chunks
  push downstream as `TTSAudioRawFrame` bracketed by `TTSStartedFrame` /
  `TTSStoppedFrame`, with a `TTSTextFrame` carrying the spoken text.

  On interruption the sentence queue clears, the in-flight context is
  cancelled server-side, and any late chunks for it are dropped.

  Options: `:api_key` (defaults to `CARTESIA_API_KEY`), `:voice_id`, `:model`
  (default `sonic-2`), `:url` (override for tests).
  """

  use Feline.Processor

  alias Feline.WebSocketClient

  alias Feline.Frames.All.{
    InterruptionFrame,
    TextFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSTextFrame
  }

  require Logger

  @cartesia_version "2025-04-16"

  @impl true
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.get(opts, :api_key) || System.get_env("CARTESIA_API_KEY"),
       voice_id:
         Keyword.get(opts, :voice_id) || System.get_env("CARTESIA_VOICE_ID") ||
           "a167e0f3-df7e-4d52-a9c3-f949145efdab",
       model: Keyword.get(opts, :model, "sonic-2"),
       url: Keyword.get(opts, :url),
       sample_rate: 24_000,
       client: nil,
       current_context: nil,
       queue: :queue.new(),
       context_counter: 0
     }}
  end

  @impl true
  def handle_setup(start_frame, _ctx, state) do
    url =
      state.url ||
        "wss://api.cartesia.ai/tts/websocket?" <>
          URI.encode_query(api_key: state.api_key, cartesia_version: @cartesia_version)

    {:ok, client} = WebSocketClient.start_link(url)
    {:ok, %{state | client: client, sample_rate: start_frame.audio_out_sample_rate}}
  end

  @impl true
  def handle_frame(%TextFrame{text: text}, :downstream, ctx, state) do
    if state.current_context do
      {:ok, %{state | queue: :queue.in(text, state.queue)}}
    else
      {:ok, synthesize(text, ctx, state)}
    end
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, _ctx, state) do
    if state.current_context do
      WebSocketClient.send_frame(
        state.client,
        {:text, Jason.encode!(%{context_id: state.current_context, cancel: true})}
      )
    end

    {:push, frame, direction, %{state | current_context: nil, queue: :queue.new()}}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  @impl true
  def handle_info({:websocket_frame, _client, {:text, json}}, ctx, state) do
    message = Jason.decode!(json)

    if message["context_id"] == state.current_context do
      handle_tts_message(message, ctx, state)
    else
      {:ok, state}
    end
  end

  def handle_info({:websocket_closed, _client, reason}, _ctx, state) do
    Logger.warning("Cartesia connection closed: #{inspect(reason)}")
    {:ok, %{state | client: nil}}
  end

  def handle_info(_message, _ctx, state), do: {:ok, state}

  @impl true
  def handle_cleanup(%{client: client}) when is_pid(client) do
    if Process.alive?(client), do: WebSocketClient.close(client)
  end

  def handle_cleanup(_state), do: :ok

  # -- Internal --

  defp handle_tts_message(%{"type" => "chunk", "data" => base64_audio}, ctx, state) do
    audio = Base.decode64!(base64_audio)

    ctx.push.(
      %TTSAudioRawFrame{audio: audio, sample_rate: state.sample_rate, num_channels: 1},
      :downstream
    )

    {:ok, state}
  end

  defp handle_tts_message(%{"type" => "done"}, ctx, state) do
    ctx.push.(%TTSStoppedFrame{}, :downstream)
    state = %{state | current_context: nil}

    case :queue.out(state.queue) do
      {{:value, text}, queue} -> {:ok, synthesize(text, ctx, %{state | queue: queue})}
      {:empty, _queue} -> {:ok, state}
    end
  end

  defp handle_tts_message(%{"type" => "error"} = message, ctx, state) do
    Logger.warning("Cartesia error: #{inspect(message)}")
    ctx.push.(%TTSStoppedFrame{}, :downstream)
    {:ok, %{state | current_context: nil}}
  end

  defp handle_tts_message(_message, _ctx, state), do: {:ok, state}

  defp synthesize(text, ctx, state) do
    context_id = "ctx-#{state.context_counter}"

    request = %{
      model_id: state.model,
      voice: %{mode: "id", id: state.voice_id},
      transcript: text,
      context_id: context_id,
      output_format: %{container: "raw", encoding: "pcm_s16le", sample_rate: state.sample_rate},
      continue: false
    }

    WebSocketClient.send_frame(state.client, {:text, Jason.encode!(request)})
    ctx.push.(%TTSStartedFrame{}, :downstream)
    ctx.push.(%TTSTextFrame{text: text}, :downstream)

    %{state | current_context: context_id, context_counter: state.context_counter + 1}
  end
end
