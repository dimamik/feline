defmodule Feline.Services.ElevenLabs.StreamingTTS do
  @moduledoc """
  ElevenLabs streaming text-to-speech via WebSocket. Sends text chunks
  and receives audio data in real time, pushing TTSAudioRawFrame as
  chunks arrive.
  """
  use Feline.Processor
  require Logger

  alias Feline.Frames.{
    TextFrame,
    LLMTextFrame,
    LLMFullResponseEndFrame,
    InterruptionFrame,
    EndFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame
  }

  alias Feline.Services.ElevenLabs.StreamingTTS.WsClient

  @default_model "eleven_turbo_v2_5"
  @default_output_format "pcm_24000"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       voice_id: Keyword.fetch!(opts, :voice_id),
       model_id: Keyword.get(opts, :model_id, @default_model),
       output_format: Keyword.get(opts, :output_format, @default_output_format),
       sample_rate: Keyword.get(opts, :sample_rate, 24_000),
       text_buffer: "",
       speaking: false,
       ws_pid: nil
     }}
  end

  @impl Feline.Processor
  def handle_frame(%frame_mod{text: text}, :downstream, push_fn, state)
      when frame_mod in [TextFrame, LLMTextFrame] do
    Logger.debug("[TTS] got #{frame_mod} text=#{inspect(String.slice(text, 0, 60))}")
    state = ensure_connected(state)
    state = %{state | text_buffer: state.text_buffer <> text}

    if sentence_end?(state.text_buffer) do
      Logger.debug("[TTS] sending text=#{inspect(String.slice(state.text_buffer, 0, 80))}")
      send_text(state.text_buffer, state)

      unless state.speaking do
        push_fn.(%TTSStartedFrame{id: make_ref()}, :downstream)
      end

      {:ok, %{state | text_buffer: "", speaking: true}}
    else
      {:ok, state}
    end
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, direction, push_fn, state) do
    # Flush any remaining buffered text
    state =
      if state.text_buffer != "" do
        send_text(state.text_buffer, state)

        unless state.speaking do
          push_fn.(%TTSStartedFrame{id: make_ref()}, :downstream)
        end

        %{state | text_buffer: "", speaking: true}
      else
        state
      end

    # Send EOS to trigger final audio generation; isFinal will emit TTSStoppedFrame
    flush_and_close(state)
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    flush_and_close(state)
    close_ws(state)
    push_fn.(frame, direction)
    {:ok, %{state | ws_pid: nil, speaking: false, text_buffer: ""}}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    state = flush_and_close(state)
    push_fn.(frame, direction)
    {:ok, %{state | speaking: false, text_buffer: ""}}
  end

  def handle_frame(frame, direction, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  @impl Feline.Processor
  def handle_info({:tts_audio, audio}, push_fn, state) do
    Logger.debug("[TTS] got audio chunk #{byte_size(audio)} bytes")

    push_fn.(
      %TTSAudioRawFrame{
        id: make_ref(),
        audio: audio,
        sample_rate: state.sample_rate
      },
      :downstream
    )

    {:ok, state}
  end

  def handle_info(:tts_stream_end, push_fn, state) do
    Logger.debug("[TTS] stream end, speaking=#{state.speaking}")

    if state.speaking do
      push_fn.(%TTSStoppedFrame{id: make_ref()}, :downstream)
    end

    {:ok, %{state | speaking: false}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, _push_fn, %{ws_pid: pid} = state) do
    Logger.debug("[TTS] WS process DOWN")
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

  defp ensure_connected(%{ws_pid: pid} = state) when is_pid(pid) do
    if Process.alive?(pid), do: state, else: open_connection(state)
  end

  defp ensure_connected(state), do: open_connection(state)

  defp open_connection(state) do
    url = build_url(state)

    case WsClient.start_link(url, state.api_key, self()) do
      {:ok, pid} ->
        Logger.debug("[TTS] WS connected, pid=#{inspect(pid)}")
        Process.monitor(pid)
        %{state | ws_pid: pid}

      {:error, reason} ->
        Logger.warning("[TTS] WS connect FAILED: #{inspect(reason)}")
        %{state | ws_pid: nil}
    end
  end

  defp build_url(state) do
    params = URI.encode_query(model_id: state.model_id, output_format: state.output_format)

    "wss://api.elevenlabs.io/v1/text-to-speech/#{state.voice_id}/stream-input?#{params}"
  end

  defp send_text(text, %{ws_pid: pid}) when is_pid(pid) do
    msg = Jason.encode!(%{text: text, flush: true})

    case WebSockex.send_frame(pid, {:text, msg}) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp send_text(_text, _state), do: :ok

  defp flush_and_close(%{ws_pid: pid} = state) when is_pid(pid) do
    msg = Jason.encode!(%{text: ""})

    case WebSockex.send_frame(pid, {:text, msg}) do
      :ok -> :ok
      {:error, _} -> :ok
    end

    state
  end

  defp flush_and_close(state), do: state

  defp close_ws(%{ws_pid: pid}) when is_pid(pid) do
    WebSockex.send_frame(pid, :close)
  end

  defp close_ws(_state), do: :ok

  defp sentence_end?(text) do
    String.ends_with?(text, ".") or
      String.ends_with?(text, "!") or
      String.ends_with?(text, "?") or
      String.ends_with?(text, "\n")
  end
end

defmodule Feline.Services.ElevenLabs.StreamingTTS.WsClient do
  @moduledoc false
  use WebSockex
  require Logger

  def start_link(url, api_key, owner) do
    WebSockex.start_link(url, __MODULE__, %{owner: owner},
      extra_headers: [{"xi-api-key", api_key}]
    )
  end

  @impl true
  def handle_connect(_conn, state) do
    send(self(), :send_bos)
    {:ok, state}
  end

  @impl true
  def handle_info(:send_bos, state) do
    Logger.debug("[WsClient] sending BOS")
    bos = Jason.encode!(%{text: " "})
    {:reply, {:text, bos}, state}
  end

  def handle_info(_msg, state) do
    {:ok, state}
  end

  @impl true
  def handle_frame({:text, msg}, state) do
    case Jason.decode(msg) do
      {:ok, %{"audio" => audio}} when is_binary(audio) and audio != "" ->
        case Base.decode64(audio) do
          {:ok, decoded} ->
            Logger.debug("[WsClient] audio chunk #{byte_size(decoded)} bytes")
            send(state.owner, {:tts_audio, decoded})

          :error ->
            Logger.warning("[WsClient] base64 decode error")
        end

      {:ok, %{"isFinal" => true}} ->
        Logger.debug("[WsClient] isFinal received")
        send(state.owner, :tts_stream_end)

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
