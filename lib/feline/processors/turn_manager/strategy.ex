defmodule Feline.Processors.TurnManager.Strategy do
  @moduledoc """
  Behaviour for pluggable turn detection strategies.

  A strategy receives frames and returns events (speaking frames,
  interruption frames) to be emitted alongside the original frame.
  """

  @callback init(opts :: keyword()) :: {:ok, state :: term()}

  @callback handle_frame(frame :: struct(), state :: term()) ::
              {:events, [struct()], state :: term()}
              | {:ok, state :: term()}
end
