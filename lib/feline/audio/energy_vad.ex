defmodule Feline.Audio.EnergyVAD do
  @moduledoc """
  RMS-energy voice activity detection over 16-bit PCM input audio.

  Emits `VADUserStartedSpeakingFrame` after `start_secs` of continuous speech
  and `VADUserStoppedSpeakingFrame` after `stop_secs` of continuous silence,
  passing all frames through.

  Options: `:threshold` (RMS on int16 scale, default 500), `:start_secs`
  (default 0.2), `:stop_secs` (default 0.8).
  """

  # ponytail: energy threshold VAD - fine for demos and quiet rooms; swap in a
  # Silero ONNX analyzer behind the same frames when accuracy matters.

  use Feline.Processor

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       threshold: Keyword.get(opts, :threshold, 500),
       start_secs: Keyword.get(opts, :start_secs, 0.2),
       stop_secs: Keyword.get(opts, :stop_secs, 0.8),
       speaking?: false,
       run_secs: 0.0
     }}
  end

  @impl true
  def handle_frame(%InputAudioRawFrame{} = frame, :downstream, _ctx, state) do
    seconds = byte_size(frame.audio) / 2 / frame.sample_rate
    speech? = rms(frame.audio) >= state.threshold

    case transition(state, speech?, seconds) do
      {:started, state} ->
        {:push_many, [{frame, :downstream}, {%VADUserStartedSpeakingFrame{}, :downstream}], state}

      {:stopped, state} ->
        {:push_many, [{frame, :downstream}, {%VADUserStoppedSpeakingFrame{}, :downstream}], state}

      {:silent, state} ->
        {:push, frame, :downstream, state}
    end
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp transition(%{speaking?: false} = state, true, seconds) do
    run_secs = state.run_secs + seconds

    if run_secs >= state.start_secs do
      {:started, %{state | speaking?: true, run_secs: 0.0}}
    else
      {:silent, %{state | run_secs: run_secs}}
    end
  end

  defp transition(%{speaking?: true} = state, false, seconds) do
    run_secs = state.run_secs + seconds

    if run_secs >= state.stop_secs do
      {:stopped, %{state | speaking?: false, run_secs: 0.0}}
    else
      {:silent, %{state | run_secs: run_secs}}
    end
  end

  defp transition(state, _speech?, _seconds), do: {:silent, %{state | run_secs: 0.0}}

  defp rms(<<>>), do: 0.0

  defp rms(audio) do
    samples = for <<sample::little-signed-16 <- audio>>, do: sample
    :math.sqrt(Enum.sum_by(samples, &(&1 * &1)) / length(samples))
  end
end
