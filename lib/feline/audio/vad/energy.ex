defmodule Feline.Audio.VAD.Energy do
  @moduledoc """
  Simple energy-based Voice Activity Detection.
  Computes RMS of audio chunk and compares to threshold.
  """

  alias Feline.Audio.Utils

  defstruct threshold: 100.0

  def new(opts \\ []) do
    %__MODULE__{threshold: Keyword.get(opts, :threshold, 100.0)}
  end

  def analyze(audio, _sample_rate, %__MODULE__{threshold: threshold} = state) do
    rms = Utils.compute_rms(audio)
    if rms > threshold, do: {:speaking, state}, else: {:quiet, state}
  end
end
