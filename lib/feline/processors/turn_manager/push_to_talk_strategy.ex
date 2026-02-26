defmodule Feline.Processors.TurnManager.PushToTalkStrategy do
  @moduledoc """
  Push-to-talk turn detection strategy. User explicitly signals
  start/stop of speech via InputTransportMessageFrame payloads.
  """

  @behaviour Feline.Processors.TurnManager.Strategy

  alias Feline.Frames.{
    InputTransportMessageFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    InterruptionFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       bot_speaking: false,
       interrupt_on_speech: Keyword.get(opts, :interrupt_on_speech, true)
     }}
  end

  @impl true
  def handle_frame(
        %InputTransportMessageFrame{payload: %{"type" => "start_speaking"}},
        state
      ) do
    events = [%UserStartedSpeakingFrame{id: make_ref()}]

    events =
      if state.bot_speaking and state.interrupt_on_speech do
        events ++ [%InterruptionFrame{id: make_ref()}]
      else
        events
      end

    {:events, events, state}
  end

  def handle_frame(
        %InputTransportMessageFrame{payload: %{"type" => "stop_speaking"}},
        state
      ) do
    {:events, [%UserStoppedSpeakingFrame{id: make_ref()}], state}
  end

  def handle_frame(%BotStartedSpeakingFrame{}, state) do
    {:ok, %{state | bot_speaking: true}}
  end

  def handle_frame(%BotStoppedSpeakingFrame{}, state) do
    {:ok, %{state | bot_speaking: false}}
  end

  def handle_frame(_frame, state) do
    {:ok, state}
  end
end
