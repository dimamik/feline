defmodule Feline.Transports.Input do
  @moduledoc """
  Base input transport processor. Passes through all frames.

  To inject audio into the pipeline, use `Feline.Processor.queue_frame/3`
  with an `InputAudioRawFrame` from outside the pipeline.
  """
  use Feline.Processor

  @impl true
  def init(opts) do
    {:ok,
     %{
       sample_rate: Keyword.get(opts, :sample_rate, 16_000),
       num_channels: Keyword.get(opts, :num_channels, 1)
     }}
  end

  @impl true
  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end
end
