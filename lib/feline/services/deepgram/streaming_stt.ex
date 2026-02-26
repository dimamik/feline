defmodule Feline.Services.Deepgram.StreamingSTT do
  @moduledoc """
  Deepgram streaming speech-to-text via WebSocket. Sends audio chunks
  as binary frames and receives JSON transcription results in real time.
  """
  use Feline.Processor

  alias Feline.Frames.{
    InputAudioRawFrame,
    InterruptionFrame,
    TranscriptionFrame,
    InterimTranscriptionFrame,
    EndFrame
  }

  alias Feline.Services.Deepgram.StreamingSTT.WsClient

  @default_model "nova-2"
  @default_language "en"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       model: Keyword.get(opts, :model, @default_model),
       language: Keyword.get(opts, :language, @default_language),
       sample_rate: Keyword.get(opts, :sample_rate, 16_000),
       encoding: Keyword.get(opts, :encoding, "linear16"),
       channels: Keyword.get(opts, :channels, 1),
       interim_results: Keyword.get(opts, :interim_results, true),
       vad_events: Keyword.get(opts, :vad_events, false),
       ws_pid: nil
     }}
  end

  @impl Feline.Processor
  def handle_frame(%InputAudioRawFrame{audio: audio}, :downstream, push_fn, state) do
    state = ensure_connected(state, push_fn)

    state =
      case send_ws(state.ws_pid, {:binary, audio}) do
        :ok -> state
        :error -> %{state | ws_pid: nil}
      end

    {:ok, state}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    close_ws(state)
    push_fn.(frame, direction)
    {:ok, %{state | ws_pid: nil}}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(frame, direction, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  @impl Feline.Processor
  def handle_info({:deepgram_transcript, result}, push_fn, state) do
    case extract_transcript(result) do
      {:final, text, language} when text != "" ->
        push_fn.(
          %TranscriptionFrame{
            id: make_ref(),
            text: text,
            language: language || state.language
          },
          :downstream
        )

      {:interim, text, language} when text != "" ->
        push_fn.(
          %InterimTranscriptionFrame{
            id: make_ref(),
            text: text,
            language: language || state.language
          },
          :downstream
        )

      _ ->
        :ok
    end

    {:ok, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, _push_fn, %{ws_pid: pid} = state) do
    {:ok, %{state | ws_pid: nil}}
  end

  def handle_info(_msg, _push_fn, state) do
    {:ok, state}
  end

  @impl Feline.Processor
  def handle_cleanup(state) do
    close_ws(state)
    :ok
  end

  defp ensure_connected(%{ws_pid: pid} = state, _push_fn) when is_pid(pid) do
    if Process.alive?(pid), do: state, else: connect(state)
  end

  defp ensure_connected(state, _push_fn), do: connect(state)

  defp connect(state) do
    url = build_url(state)

    case WsClient.start_link(url, state.api_key, self()) do
      {:ok, pid} ->
        Process.monitor(pid)
        %{state | ws_pid: pid}

      {:error, _reason} ->
        %{state | ws_pid: nil}
    end
  end

  defp build_url(state) do
    params =
      URI.encode_query(
        model: state.model,
        language: state.language,
        encoding: state.encoding,
        sample_rate: state.sample_rate,
        channels: state.channels,
        interim_results: state.interim_results,
        vad_events: state.vad_events
      )

    "wss://api.deepgram.com/v1/listen?#{params}"
  end

  defp extract_transcript(result) do
    channel =
      result
      |> Map.get("channel", %{})
      |> Map.get("alternatives", [])
      |> List.first(%{})

    text = Map.get(channel, "transcript", "")
    language = get_in(result, ["channel", "detected_language"])
    is_final = Map.get(result, "is_final", false)

    if is_final, do: {:final, text, language}, else: {:interim, text, language}
  end

  defp send_ws(pid, frame) when is_pid(pid) do
    case WebSockex.send_frame(pid, frame) do
      :ok -> :ok
      {:error, _} -> :error
    end
  end

  defp send_ws(nil, _frame), do: :error

  defp close_ws(%{ws_pid: pid}) when is_pid(pid) do
    WebSockex.send_frame(pid, :close)
  end

  defp close_ws(_state), do: :ok
end

defmodule Feline.Services.Deepgram.StreamingSTT.WsClient do
  @moduledoc false
  use WebSockex

  def start_link(url, api_key, owner) do
    WebSockex.start_link(url, __MODULE__, %{owner: owner},
      extra_headers: [{"Authorization", "Token #{api_key}"}]
    )
  end

  @impl true
  def handle_frame({:text, msg}, state) do
    case Jason.decode(msg) do
      {:ok, %{"type" => "Results"} = result} ->
        send(state.owner, {:deepgram_transcript, result})

      _ ->
        :ok
    end

    {:ok, state}
  end

  def handle_frame(_frame, state), do: {:ok, state}

  @impl true
  def handle_disconnect(_status, state) do
    {:ok, state}
  end
end
