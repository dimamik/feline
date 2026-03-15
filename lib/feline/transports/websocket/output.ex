defmodule Feline.Transports.WebSocket.Output do
  @moduledoc """
  Processor that buffers TTS audio and sends it in timed chunks over
  WebSocket. Emits `BotStartedSpeakingFrame` / `BotStoppedSpeakingFrame`
  to coordinate echo suppression.
  """
  use Feline.Processor

  alias Feline.Frames.{
    OutputAudioRawFrame,
    TTSAudioRawFrame,
    OutputTransportMessageFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InterruptionFrame,
    EndFrame
  }

  alias Feline.RTVI.Messages
  alias Feline.TransportParams

  @impl true
  def init(opts) do
    params = Keyword.get(opts, :params, %TransportParams{})
    ws_pid = Keyword.get(opts, :ws_pid)
    rtvi_enabled = Keyword.get(opts, :rtvi_enabled, false)
    chunk_bytes = compute_chunk_bytes(params)

    {:ok,
     %{
       params: params,
       ws_pid: ws_pid,
       rtvi_enabled: rtvi_enabled,
       audio_buffer: <<>>,
       chunk_bytes: chunk_bytes,
       speaking: false,
       send_timer: nil
     }}
  end

  @impl true
  def handle_frame(%frame_mod{audio: audio}, :downstream, push_fn, state)
      when frame_mod in [OutputAudioRawFrame, TTSAudioRawFrame] do
    if state.params.audio_out_enabled do
      state =
        if state.speaking do
          state
        else
          push_fn.(%BotStartedSpeakingFrame{id: make_ref()}, :downstream)
          maybe_send_rtvi(state, Messages.bot_started_speaking())

          state
          |> schedule_send()
          |> Map.put(:speaking, true)
        end

      {:ok, %{state | audio_buffer: state.audio_buffer <> audio}}
    else
      {:ok, state}
    end
  end

  def handle_frame(%OutputTransportMessageFrame{payload: payload}, :downstream, _push_fn, state) do
    send_message(payload, state)
    {:ok, state}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    state = flush_audio(state)
    state = stop_speaking(state, push_fn)
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    state = drain_audio(state)
    state = stop_speaking(state, push_fn)
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  @impl true
  def handle_info(:send_audio_chunk, push_fn, state) do
    if byte_size(state.audio_buffer) >= state.chunk_bytes do
      <<chunk::binary-size(state.chunk_bytes), rest::binary>> = state.audio_buffer
      send_audio(chunk, state)
      state = %{state | audio_buffer: rest}
      {:ok, schedule_send(state)}
    else
      if state.speaking and byte_size(state.audio_buffer) == 0 do
        {:ok, stop_speaking(state, push_fn)}
      else
        {:ok, schedule_send(state)}
      end
    end
  end

  def handle_info(_msg, _push_fn, state), do: {:ok, state}

  # 10ms of 16-bit PCM mono audio * chunks_per_send
  defp compute_chunk_bytes(params) do
    div(params.audio_out_sample_rate, 100) * 2 * params.audio_out_10ms_chunks
  end

  defp schedule_send(state) do
    if state.send_timer, do: Process.cancel_timer(state.send_timer)
    interval_ms = state.params.audio_out_10ms_chunks * 10
    timer = Process.send_after(self(), :send_audio_chunk, interval_ms)
    %{state | send_timer: timer}
  end

  defp send_audio(chunk, %{ws_pid: pid}) when is_pid(pid) do
    send(pid, {:send_audio, chunk})
  end

  defp send_audio(_chunk, _state), do: :ok

  defp send_message(payload, %{ws_pid: pid}) when is_pid(pid) do
    send(pid, {:send_message, payload})
  end

  defp send_message(_payload, _state), do: :ok

  defp flush_audio(state) do
    if state.send_timer, do: Process.cancel_timer(state.send_timer)
    %{state | audio_buffer: <<>>, send_timer: nil}
  end

  defp drain_audio(state) do
    if byte_size(state.audio_buffer) > 0, do: send_audio(state.audio_buffer, state)
    if state.send_timer, do: Process.cancel_timer(state.send_timer)
    %{state | audio_buffer: <<>>, send_timer: nil}
  end

  defp stop_speaking(state, push_fn) do
    if state.speaking do
      push_fn.(%BotStoppedSpeakingFrame{id: make_ref()}, :downstream)
      maybe_send_rtvi(state, Messages.bot_stopped_speaking())
    end

    %{state | speaking: false}
  end

  defp maybe_send_rtvi(%{rtvi_enabled: true} = state, payload) do
    send_message(payload, state)
  end

  defp maybe_send_rtvi(_state, _payload), do: :ok
end
