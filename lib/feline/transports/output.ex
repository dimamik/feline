defmodule Feline.Transports.Output do
  @moduledoc """
  Base output transport processor. Receives OutputAudioRawFrame and
  TTSAudioRawFrame and forwards the audio data to a callback function
  or registered process.
  """
  use Feline.Processor

  alias Feline.Frames.{OutputAudioRawFrame, TTSAudioRawFrame}

  @impl true
  def init(opts) do
    {:ok,
     %{
       audio_handler: Keyword.get(opts, :audio_handler),
       sample_rate: Keyword.get(opts, :sample_rate, 24_000)
     }}
  end

  @impl true
  def handle_frame(%OutputAudioRawFrame{audio: audio}, :downstream, _push_fn, state) do
    deliver_audio(audio, state)
    {:ok, state}
  end

  def handle_frame(%TTSAudioRawFrame{audio: audio}, :downstream, _push_fn, state) do
    deliver_audio(audio, state)
    {:ok, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp deliver_audio(audio, %{audio_handler: handler}) when is_function(handler, 1) do
    handler.(audio)
  end

  defp deliver_audio(audio, %{audio_handler: pid}) when is_pid(pid) do
    send(pid, {:audio_out, audio})
  end

  defp deliver_audio(_audio, _state), do: :ok
end
