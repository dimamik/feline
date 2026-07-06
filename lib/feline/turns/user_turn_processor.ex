defmodule Feline.Turns.UserTurnProcessor do
  @moduledoc """
  Turns VAD speech events into user turns, mirroring pipecat's turn engine:
  on turn start it broadcasts `UserStartedSpeakingFrame` and an
  `InterruptionFrame` in both directions (barge-in: every processor drops
  in-flight interruptible work); on turn stop it broadcasts
  `UserStoppedSpeakingFrame`.

  Option `:interrupt_on_start` (default true) controls barge-in.
  """

  # ponytail: VAD-driven start/stop only; pluggable start/stop/mute strategy
  # behaviours (smart turn, min-words, wake phrase) can slot in here later.

  use Feline.Processor

  alias Feline.Frames.All.{
    InterruptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame
  }

  @impl true
  def init(opts) do
    {:ok, %{interrupt_on_start: Keyword.get(opts, :interrupt_on_start, true)}}
  end

  @impl true
  def handle_frame(%VADUserStartedSpeakingFrame{} = frame, direction, _ctx, state) do
    started = %UserStartedSpeakingFrame{}

    pushes =
      [{frame, direction}, {started, :downstream}, {started, :upstream}] ++
        if state.interrupt_on_start do
          [{%InterruptionFrame{}, :downstream}, {%InterruptionFrame{}, :upstream}]
        else
          []
        end

    {:push_many, pushes, state}
  end

  def handle_frame(%VADUserStoppedSpeakingFrame{} = frame, direction, _ctx, state) do
    stopped = %UserStoppedSpeakingFrame{}
    {:push_many, [{frame, direction}, {stopped, :downstream}, {stopped, :upstream}], state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
end
