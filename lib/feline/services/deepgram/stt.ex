defmodule Feline.Services.Deepgram.STT do
  @moduledoc """
  Streaming speech-to-text over Deepgram's websocket API. Input audio is
  forwarded to Deepgram as it flows through; results come back as
  `TranscriptionFrame` (final) / `InterimTranscriptionFrame` pushed
  downstream. Audio passes through untouched.

  Options: `:api_key` (defaults to `DEEPGRAM_API_KEY`), `:url` (override for
  tests/self-hosting), `:model` (default `nova-2`).
  """

  use Feline.Processor

  alias Feline.WebSocketClient

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame
  }

  require Logger

  @impl true
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.get(opts, :api_key) || System.get_env("DEEPGRAM_API_KEY"),
       url: Keyword.get(opts, :url),
       model: Keyword.get(opts, :model, "nova-2"),
       client: nil
     }}
  end

  @impl true
  def handle_setup(start_frame, _ctx, state) do
    query =
      URI.encode_query(
        encoding: "linear16",
        sample_rate: start_frame.audio_in_sample_rate,
        channels: 1,
        model: state.model,
        interim_results: true,
        punctuate: true
      )

    url = state.url || "wss://api.deepgram.com/v1/listen?#{query}"

    {:ok, client} =
      WebSocketClient.start_link(url, headers: [{"authorization", "Token #{state.api_key}"}])

    {:ok, %{state | client: client}}
  end

  @impl true
  def handle_frame(%InputAudioRawFrame{} = frame, :downstream, _ctx, state) do
    WebSocketClient.send_frame(state.client, {:binary, frame.audio})
    {:push, frame, :downstream, state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  @impl true
  def handle_info({:websocket_frame, _client, {:text, json}}, ctx, state) do
    case Jason.decode(json) do
      {:ok,
       %{
         "channel" => %{"alternatives" => [%{"transcript" => transcript} | _rest]},
         "is_final" => final?
       }}
      when transcript != "" ->
        frame_module = if final?, do: TranscriptionFrame, else: InterimTranscriptionFrame

        frame =
          struct!(frame_module,
            text: transcript,
            timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
          )

        ctx.push.(frame, :downstream)

      _other ->
        :ok
    end

    {:ok, state}
  end

  def handle_info({:websocket_closed, _client, reason}, _ctx, state) do
    Logger.warning("Deepgram connection closed: #{inspect(reason)}")
    {:ok, %{state | client: nil}}
  end

  def handle_info(_message, _ctx, state), do: {:ok, state}

  @impl true
  def handle_cleanup(%{client: client}) when is_pid(client) do
    if Process.alive?(client), do: WebSocketClient.close(client)
  end

  def handle_cleanup(_state), do: :ok
end
