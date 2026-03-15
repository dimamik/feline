defmodule Feline.Services.Deepgram.StreamingTTS do
  @moduledoc """
  Deepgram streaming text-to-speech via WebSocket. Sends text chunks
  and receives audio data in real time, pushing TTSAudioRawFrame as
  chunks arrive.
  """
  use Feline.Processor

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

  alias Feline.Services.Deepgram.StreamingTTS.WsClient

  @default_voice "aura-2-draco-en"
  @default_encoding "linear16"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       voice: Keyword.get(opts, :voice, @default_voice),
       encoding: Keyword.get(opts, :encoding, @default_encoding),
       sample_rate: Keyword.get(opts, :sample_rate, 24_000),
       speaking: false,
       ws_pid: nil
     }}
  end

  @impl Feline.Processor
  def handle_frame(%frame_mod{text: text}, :downstream, push_fn, state)
      when frame_mod in [TextFrame, LLMTextFrame] do
    state = ensure_connected(state)

    unless state.speaking do
      push_fn.(%TTSStartedFrame{id: make_ref()}, :downstream)
    end

    send_speak(text, state)
    {:ok, %{state | speaking: true}}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, direction, push_fn, state) do
    send_flush(state)
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    send_flush(state)
    close_ws(state)
    push_fn.(frame, direction)
    {:ok, %{state | ws_pid: nil, speaking: false}}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    send_clear(state)
    push_fn.(frame, direction)
    {:ok, %{state | speaking: false}}
  end

  def handle_frame(frame, direction, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  @impl Feline.Processor
  def handle_info({:tts_audio, audio}, push_fn, state) do
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

  def handle_info(:tts_flushed, push_fn, state) do
    if state.speaking do
      push_fn.(%TTSStoppedFrame{id: make_ref()}, :downstream)
    end

    {:ok, %{state | speaking: false}}
  end

  def handle_info({:tts_error, message}, _push_fn, _state) do
    raise "Deepgram TTS error: #{message}"
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

  defp ensure_connected(%{ws_pid: pid} = state) when is_pid(pid) do
    if Process.alive?(pid), do: state, else: open_connection(state)
  end

  defp ensure_connected(state), do: open_connection(state)

  defp open_connection(state) do
    url = build_url(state)

    case WsClient.start_link(url, state.api_key, self()) do
      {:ok, pid} ->
        Process.monitor(pid)
        %{state | ws_pid: pid}

      {:error, reason} ->
        raise "Deepgram TTS WebSocket connection failed: #{inspect(reason)}"
    end
  end

  defp build_url(state) do
    params =
      URI.encode_query(
        model: state.voice,
        encoding: state.encoding,
        sample_rate: state.sample_rate
      )

    "wss://api.deepgram.com/v1/speak?#{params}"
  end

  defp send_speak(text, %{ws_pid: pid}) when is_pid(pid) do
    msg = Jason.encode!(%{type: "Speak", text: text})

    case WebSockex.send_frame(pid, {:text, msg}) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp send_speak(_text, _state), do: :ok

  defp send_flush(%{ws_pid: pid}) when is_pid(pid) do
    msg = Jason.encode!(%{type: "Flush"})

    case WebSockex.send_frame(pid, {:text, msg}) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp send_flush(_state), do: :ok

  defp send_clear(%{ws_pid: pid}) when is_pid(pid) do
    msg = Jason.encode!(%{type: "Clear"})

    case WebSockex.send_frame(pid, {:text, msg}) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp send_clear(_state), do: :ok

  defp close_ws(%{ws_pid: pid}) when is_pid(pid) do
    msg = Jason.encode!(%{type: "Close"})
    WebSockex.send_frame(pid, {:text, msg})
  end

  defp close_ws(_state), do: :ok
end

defmodule Feline.Services.Deepgram.StreamingTTS.WsClient do
  @moduledoc false
  use WebSockex
  require Logger

  def start_link(url, api_key, owner) do
    Logger.debug("[DG-TTS] connecting to #{url}")

    result =
      WebSockex.start_link(url, __MODULE__, %{owner: owner},
        extra_headers: [{"Authorization", "Token #{api_key}"}]
      )

    case result do
      {:ok, pid} -> Logger.debug("[DG-TTS] connected, pid=#{inspect(pid)}")
      {:error, err} -> Logger.error("[DG-TTS] connect failed: #{inspect(err)}")
    end

    result
  end

  @impl true
  def handle_frame({:binary, audio}, state) do
    Logger.debug("[DG-TTS] audio chunk #{byte_size(audio)} bytes")
    send(state.owner, {:tts_audio, audio})
    {:ok, state}
  end

  def handle_frame({:text, msg}, state) do
    Logger.debug("[DG-TTS] text msg: #{String.slice(msg, 0, 200)}")

    case Jason.decode(msg) do
      {:ok, %{"type" => "Flushed"}} ->
        send(state.owner, :tts_flushed)

      {:ok, %{"type" => "Warning", "description" => desc}} ->
        Logger.warning("[DG-TTS] #{desc}")
        send(state.owner, {:tts_error, desc})

      _ ->
        :ok
    end

    {:ok, state}
  end

  def handle_frame(_frame, state), do: {:ok, state}

  @impl true
  def handle_disconnect(status, state) do
    case status do
      %{reason: {:remote, _code, message}} ->
        Logger.error("[DG-TTS] disconnected: #{message}")
        send(state.owner, {:tts_error, message})

      _ ->
        :ok
    end

    {:ok, state}
  end
end
