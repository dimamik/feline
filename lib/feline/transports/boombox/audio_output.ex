defmodule Feline.Transports.Boombox.AudioOutput do
  @moduledoc """
  Processor that sends TTS audio to a Boombox writer for WebRTC output.

  Uses async message-based writing to avoid blocking the processor
  GenServer on Boombox's demand-based back-pressure.
  """
  use Feline.Processor

  alias Feline.Frames.{
    TTSAudioRawFrame,
    TTSStoppedFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    EndFrame,
    InterruptionFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       writer_pid: Keyword.fetch!(opts, :writer).server_reference,
       sample_rate: Keyword.get(opts, :sample_rate, 24_000),
       speaking: false,
       pts: 0
     }}
  end

  @impl true
  def handle_frame(%TTSAudioRawFrame{audio: audio} = frame, :downstream, push_fn, state) do
    state =
      unless state.speaking do
        push_fn.(%BotStartedSpeakingFrame{id: make_ref()}, :upstream)
        %{state | speaking: true}
      else
        state
      end

    packet = %Boombox.Packet{
      payload: audio,
      kind: :audio,
      pts: Membrane.Time.milliseconds(state.pts),
      format: %{
        audio_format: :s16le,
        audio_channels: 1,
        audio_rate: state.sample_rate
      }
    }

    send(state.writer_pid, {:boombox_packet, packet})

    duration_ms = div(byte_size(audio) * 1000, state.sample_rate * 2)
    {:push, frame, :downstream, %{state | pts: state.pts + duration_ms}}
  end

  def handle_frame(%TTSStoppedFrame{} = frame, :downstream, push_fn, state) do
    state = stop_speaking(state, push_fn)
    {:push, frame, :downstream, state}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    state = stop_speaking(state, push_fn)
    {:push, frame, direction, %{state | pts: 0}}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    state = stop_speaking(state, push_fn)
    send(state.writer_pid, :boombox_close)
    {:push, frame, direction, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp stop_speaking(%{speaking: false} = state, _push_fn), do: state

  defp stop_speaking(state, push_fn) do
    push_fn.(%BotStoppedSpeakingFrame{id: make_ref()}, :upstream)
    %{state | speaking: false}
  end
end
