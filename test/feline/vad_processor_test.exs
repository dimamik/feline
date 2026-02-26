defmodule Feline.Processors.VADProcessorTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.Frames.{InputAudioRawFrame, UserStartedSpeakingFrame, UserStoppedSpeakingFrame}

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
    # 1600 samples at value 1000 = 100ms at 16kHz, clearly above default threshold
    for _ <- 1..1600, into: <<>>, do: <<232, 3>>
  end

  defp silent_audio do
    # 1600 samples of silence = 100ms at 16kHz
    <<0::size(1600 * 16)>>
  end

  test "detects speech start after start_secs of speaking" do
    {:ok, vad} =
      Feline.Processor.Server.start_link(
        {Feline.Processors.VADProcessor, [start_secs: 0.15, stop_secs: 0.5]}
      )

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(vad, collector)

    for _ <- 1..2 do
      frame = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
      Processor.queue_frame(vad, frame, :downstream)
    end

    assert_receive {:collected, %UserStartedSpeakingFrame{}, :downstream}, 500
  end

  test "detects speech stop after stop_secs of silence" do
    {:ok, vad} =
      Feline.Processor.Server.start_link(
        {Feline.Processors.VADProcessor, [start_secs: 0.05, stop_secs: 0.15]}
      )

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(vad, collector)

    frame = %InputAudioRawFrame{id: make_ref(), audio: loud_audio(), sample_rate: 16_000}
    Processor.queue_frame(vad, frame, :downstream)
    assert_receive {:collected, %UserStartedSpeakingFrame{}, :downstream}, 500

    for _ <- 1..3 do
      frame = %InputAudioRawFrame{id: make_ref(), audio: silent_audio(), sample_rate: 16_000}
      Processor.queue_frame(vad, frame, :downstream)
    end

    assert_receive {:collected, %UserStoppedSpeakingFrame{}, :downstream}, 500
  end

  test "passes through audio frames" do
    {:ok, vad} =
      Feline.Processor.Server.start_link({Feline.Processors.VADProcessor, [start_secs: 10.0]})

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(vad, collector)

    frame = %InputAudioRawFrame{id: make_ref(), audio: silent_audio(), sample_rate: 16_000}
    Processor.queue_frame(vad, frame, :downstream)

    assert_receive {:collected, %InputAudioRawFrame{}, :downstream}, 500
  end
end
