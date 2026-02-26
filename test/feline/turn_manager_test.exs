defmodule Feline.Processors.TurnManagerTest do
  use ExUnit.Case, async: true

  alias Feline.Processor

  alias Feline.Processors.TurnManager
  alias Feline.Processors.TurnManager.VADStrategy
  alias Feline.Processors.TurnManager.PushToTalkStrategy

  alias Feline.Frames.{
    InputAudioRawFrame,
    InputTransportMessageFrame,
    TextFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    InterruptionFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame
  }

  defmodule Collector do
    use Feline.Processor

    @impl true
    def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      send(state.test_pid, {:collected, frame, direction})
      {:ok, state}
    end
  end

  defp loud_audio do
    for _ <- 1..1600, into: <<>>, do: <<232, 3>>
  end

  defp silent_audio do
    <<0::size(1600 * 16)>>
  end

  # --- VADStrategy unit tests ---

  describe "VADStrategy" do
    test "produces UserStartedSpeakingFrame after start_secs" do
      {:ok, state} = VADStrategy.init(start_secs: 0.15, stop_secs: 0.5)

      frame = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}

      {_state, events, _} = apply_frames(VADStrategy, state, frame, 2)

      assert Enum.any?(events, &match?(%UserStartedSpeakingFrame{}, &1))
    end

    test "produces UserStoppedSpeakingFrame after stop_secs" do
      {:ok, state} = VADStrategy.init(start_secs: 0.05, stop_secs: 0.15)

      loud = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
      silent = %InputAudioRawFrame{id: make_ref(), audio: silent_audio(), sample_rate: 16_000}

      {_, all_events, state} = apply_frames(VADStrategy, state, loud, 1)
      {_, more_events, _state} = apply_frames(VADStrategy, state, silent, 3)
      all_events = all_events ++ more_events

      assert Enum.any?(all_events, &match?(%UserStartedSpeakingFrame{}, &1))
      assert Enum.any?(all_events, &match?(%UserStoppedSpeakingFrame{}, &1))
    end

    test "produces InterruptionFrame when bot is speaking" do
      {:ok, state} = VADStrategy.init(start_secs: 0.05, interrupt_on_speech: true)

      {:ok, state} = VADStrategy.handle_frame(%BotStartedSpeakingFrame{id: make_ref()}, state)

      loud = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
      {_, events, _state} = apply_frames(VADStrategy, state, loud, 1)

      assert Enum.any?(events, &match?(%InterruptionFrame{}, &1))
    end

    test "no InterruptionFrame when interrupt_on_speech is false" do
      {:ok, state} = VADStrategy.init(start_secs: 0.05, interrupt_on_speech: false)

      {:ok, state} = VADStrategy.handle_frame(%BotStartedSpeakingFrame{id: make_ref()}, state)

      loud = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
      {_, events, _state} = apply_frames(VADStrategy, state, loud, 1)

      refute Enum.any?(events, &match?(%InterruptionFrame{}, &1))
    end

    test "returns :ok for unrelated frames" do
      {:ok, state} = VADStrategy.init([])

      assert {:ok, ^state} =
               VADStrategy.handle_frame(%TextFrame{id: make_ref(), text: "hi"}, state)
    end
  end

  # --- PushToTalkStrategy unit tests ---

  describe "PushToTalkStrategy" do
    test "produces UserStartedSpeakingFrame on start_speaking message" do
      {:ok, state} = PushToTalkStrategy.init([])

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "start_speaking"}}
      assert {:events, events, _state} = PushToTalkStrategy.handle_frame(frame, state)
      assert [%UserStartedSpeakingFrame{}] = events
    end

    test "produces UserStoppedSpeakingFrame on stop_speaking message" do
      {:ok, state} = PushToTalkStrategy.init([])

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "stop_speaking"}}
      assert {:events, events, _state} = PushToTalkStrategy.handle_frame(frame, state)
      assert [%UserStoppedSpeakingFrame{}] = events
    end

    test "produces InterruptionFrame when bot is speaking" do
      {:ok, state} = PushToTalkStrategy.init(interrupt_on_speech: true)

      {:ok, state} =
        PushToTalkStrategy.handle_frame(%BotStartedSpeakingFrame{id: make_ref()}, state)

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "start_speaking"}}
      assert {:events, events, _state} = PushToTalkStrategy.handle_frame(frame, state)

      assert Enum.any?(events, &match?(%UserStartedSpeakingFrame{}, &1))
      assert Enum.any?(events, &match?(%InterruptionFrame{}, &1))
    end

    test "no InterruptionFrame when interrupt_on_speech is false" do
      {:ok, state} = PushToTalkStrategy.init(interrupt_on_speech: false)

      {:ok, state} =
        PushToTalkStrategy.handle_frame(%BotStartedSpeakingFrame{id: make_ref()}, state)

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "start_speaking"}}
      assert {:events, events, _state} = PushToTalkStrategy.handle_frame(frame, state)

      assert [%UserStartedSpeakingFrame{}] = events
    end

    test "tracks bot stopped speaking" do
      {:ok, state} = PushToTalkStrategy.init(interrupt_on_speech: true)

      {:ok, state} =
        PushToTalkStrategy.handle_frame(%BotStartedSpeakingFrame{id: make_ref()}, state)

      {:ok, state} =
        PushToTalkStrategy.handle_frame(%BotStoppedSpeakingFrame{id: make_ref()}, state)

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "start_speaking"}}
      assert {:events, events, _state} = PushToTalkStrategy.handle_frame(frame, state)

      assert [%UserStartedSpeakingFrame{}] = events
    end

    test "returns :ok for unrelated frames" do
      {:ok, state} = PushToTalkStrategy.init([])

      assert {:ok, ^state} =
               PushToTalkStrategy.handle_frame(%TextFrame{id: make_ref(), text: "hi"}, state)
    end
  end

  # --- TurnManager integration tests (processor in pipeline) ---

  describe "TurnManager processor" do
    test "passes through all frames with VAD strategy" do
      {:ok, tm} =
        Feline.Processor.Server.start_link(
          {TurnManager, strategy: VADStrategy, strategy_opts: [start_secs: 10.0]}
        )

      {:ok, collector} =
        Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

      Processor.link(tm, collector)

      frame = %InputAudioRawFrame{id: make_ref(), audio: silent_audio(), sample_rate: 16_000}
      Processor.queue_frame(tm, frame, :downstream)

      assert_receive {:collected, %InputAudioRawFrame{}, :downstream}, 500
    end

    test "emits speaking events alongside audio via VAD strategy" do
      {:ok, tm} =
        Feline.Processor.Server.start_link(
          {TurnManager, strategy: VADStrategy, strategy_opts: [start_secs: 0.15, stop_secs: 0.5]}
        )

      {:ok, collector} =
        Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

      Processor.link(tm, collector)

      for _ <- 1..2 do
        frame = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
        Processor.queue_frame(tm, frame, :downstream)
      end

      assert_receive {:collected, %InputAudioRawFrame{}, :downstream}, 500
      assert_receive {:collected, %UserStartedSpeakingFrame{}, :downstream}, 500
    end

    test "emits speaking events via push-to-talk strategy" do
      {:ok, tm} =
        Feline.Processor.Server.start_link(
          {TurnManager, strategy: PushToTalkStrategy, strategy_opts: []}
        )

      {:ok, collector} =
        Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

      Processor.link(tm, collector)

      frame = %InputTransportMessageFrame{id: make_ref(), payload: %{"type" => "start_speaking"}}
      Processor.queue_frame(tm, frame, :downstream)

      assert_receive {:collected, %InputTransportMessageFrame{}, :downstream}, 500
      assert_receive {:collected, %UserStartedSpeakingFrame{}, :downstream}, 500
    end

    test "non-strategy frames pass through unchanged" do
      {:ok, tm} =
        Feline.Processor.Server.start_link(
          {TurnManager, strategy: PushToTalkStrategy, strategy_opts: []}
        )

      {:ok, collector} =
        Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

      Processor.link(tm, collector)

      frame = %TextFrame{id: make_ref(), text: "hello"}
      Processor.queue_frame(tm, frame, :downstream)

      assert_receive {:collected, %TextFrame{text: "hello"}, :downstream}, 500
    end
  end

  # Helper: apply the same frame N times, collecting all events
  defp apply_frames(strategy_mod, state, frame, count) do
    Enum.reduce(1..count, {state, []}, fn _, {state, acc_events} ->
      case strategy_mod.handle_frame(frame, state) do
        {:events, events, new_state} -> {new_state, acc_events ++ events}
        {:ok, new_state} -> {new_state, acc_events}
      end
    end)
    |> then(fn {state, events} -> {state, events, state} end)
  end
end
