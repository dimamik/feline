defmodule Feline.Processors.TurnManager.VADStrategy do
  @moduledoc """
  VAD-based turn detection strategy. Extracts the energy-based voice
  activity detection logic from VADProcessor into a pluggable strategy.

  Tracks :quiet/:speaking state and debounces transitions using
  configurable start_secs / stop_secs thresholds.
  """

  @behaviour Feline.Processors.TurnManager.Strategy

  alias Feline.Audio.VAD.Energy
  alias Feline.Audio.Utils

  alias Feline.Frames.{
    InputAudioRawFrame,
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
       analyzer: Keyword.get(opts, :analyzer, Energy.new()),
       state: :quiet,
       start_secs: Keyword.get(opts, :start_secs, 0.2),
       stop_secs: Keyword.get(opts, :stop_secs, 0.8),
       speaking_time: 0.0,
       quiet_time: 0.0,
       bot_speaking: false,
       interrupt_on_speech: Keyword.get(opts, :interrupt_on_speech, true)
     }}
  end

  @impl true
  def handle_frame(%InputAudioRawFrame{audio: audio, sample_rate: sr}, state) do
    chunk_duration = Utils.duration_ms(audio, sr) / 1_000.0

    {vad_result, analyzer} = Energy.analyze(audio, sr, state.analyzer)
    state = %{state | analyzer: analyzer}

    {new_vad_state, events, state} = update_state_machine(vad_result, chunk_duration, state)
    state = %{state | state: new_vad_state}

    {:events, events, state}
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

  defp update_state_machine(:speaking, chunk_duration, %{state: :quiet} = state) do
    speaking_time = state.speaking_time + chunk_duration

    if speaking_time >= state.start_secs do
      events = [%UserStartedSpeakingFrame{id: make_ref()}]

      events =
        if state.bot_speaking and state.interrupt_on_speech do
          events ++ [%InterruptionFrame{id: make_ref()}]
        else
          events
        end

      {:speaking, events, %{state | speaking_time: 0.0, quiet_time: 0.0}}
    else
      {:quiet, [], %{state | speaking_time: speaking_time}}
    end
  end

  defp update_state_machine(:quiet, chunk_duration, %{state: :speaking} = state) do
    quiet_time = state.quiet_time + chunk_duration

    if quiet_time >= state.stop_secs do
      events = [%UserStoppedSpeakingFrame{id: make_ref()}]
      {:quiet, events, %{state | quiet_time: 0.0, speaking_time: 0.0}}
    else
      {:speaking, [], %{state | quiet_time: quiet_time}}
    end
  end

  defp update_state_machine(:speaking, _chunk_duration, %{state: :speaking} = state) do
    {:speaking, [], %{state | quiet_time: 0.0}}
  end

  defp update_state_machine(:quiet, _chunk_duration, %{state: :quiet} = state) do
    {:quiet, [], %{state | speaking_time: 0.0}}
  end
end
