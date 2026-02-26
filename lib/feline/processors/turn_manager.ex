defmodule Feline.Processors.TurnManager do
  @moduledoc """
  Pluggable turn management processor. Delegates turn detection to a
  configurable strategy module that implements the
  `Feline.Processors.TurnManager.Strategy` behaviour.

  All frames pass through unchanged. The strategy may produce additional
  event frames (e.g. UserStartedSpeakingFrame, InterruptionFrame) that
  are pushed downstream alongside the original frame.

  ## Options

    * `:strategy` - module implementing the Strategy behaviour (required)
    * `:strategy_opts` - keyword list passed to `strategy.init/1` (default: `[]`)

  ## Example

      {Feline.Processors.TurnManager,
       strategy: Feline.Processors.TurnManager.VADStrategy,
       strategy_opts: [start_secs: 0.2, stop_secs: 0.8]}
  """
  use Feline.Processor

  @impl true
  def init(opts) do
    strategy = Keyword.fetch!(opts, :strategy)
    strategy_opts = Keyword.get(opts, :strategy_opts, [])
    {:ok, strategy_state} = strategy.init(strategy_opts)

    {:ok, %{strategy: strategy, strategy_state: strategy_state}}
  end

  @impl true
  def handle_frame(frame, direction, _push_fn, state) do
    case state.strategy.handle_frame(frame, state.strategy_state) do
      {:events, events, strategy_state} ->
        event_frames = Enum.map(events, &{&1, :downstream})

        {:push_many, [{frame, direction} | event_frames],
         %{state | strategy_state: strategy_state}}

      {:ok, strategy_state} ->
        {:push, frame, direction, %{state | strategy_state: strategy_state}}
    end
  end
end
