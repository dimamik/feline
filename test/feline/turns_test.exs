defmodule Feline.TurnsTest do
  use ExUnit.Case, async: true

  alias Feline.Pipeline.Task, as: PipelineTask
  alias Feline.TestProcessors.{Recorder, SlowWorker}

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InterruptionFrame,
    TextFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame
  }

  defp audio_frame(sample, count) do
    %InputAudioRawFrame{
      audio: :binary.copy(<<sample::little-signed-16>>, count),
      sample_rate: 16_000
    }
  end

  # 1600 samples @16kHz = 100ms per frame
  defp loud, do: audio_frame(5_000, 1_600)
  defp silent, do: audio_frame(0, 1_600)

  test "energy VAD emits started after start_secs and stopped after stop_secs" do
    {:ok, task} =
      PipelineTask.start_link(
        processors: [{Feline.Audio.EnergyVAD, start_secs: 0.2, stop_secs: 0.3}],
        subscriber: self()
      )

    for _index <- 1..3, do: PipelineTask.queue_frame(task, loud())
    assert_receive {:feline_pipeline, :downstream, %VADUserStartedSpeakingFrame{}}

    for _index <- 1..4, do: PipelineTask.queue_frame(task, silent())
    assert_receive {:feline_pipeline, :downstream, %VADUserStoppedSpeakingFrame{}}
  end

  test "turn start broadcasts user-started and interruption both directions" do
    {:ok, task} =
      PipelineTask.start_link(
        processors: [
          {Recorder, listener: self(), tag: :upstream_side},
          Feline.Turns.UserTurnProcessor,
          {Recorder, listener: self(), tag: :downstream_side}
        ],
        subscriber: self()
      )

    PipelineTask.queue_frame(task, %VADUserStartedSpeakingFrame{})

    assert_receive {:recorded, :downstream_side, %UserStartedSpeakingFrame{}, :downstream}
    assert_receive {:recorded, :downstream_side, %InterruptionFrame{}, :downstream}
    assert_receive {:recorded, :upstream_side, %UserStartedSpeakingFrame{}, :upstream}
    assert_receive {:recorded, :upstream_side, %InterruptionFrame{}, :upstream}

    PipelineTask.queue_frame(task, %VADUserStoppedSpeakingFrame{})
    assert_receive {:recorded, :downstream_side, %UserStoppedSpeakingFrame{}, :downstream}
    assert_receive {:recorded, :upstream_side, %UserStoppedSpeakingFrame{}, :upstream}
  end

  test "barge-in end-to-end: user speech kills in-flight bot work downstream" do
    {:ok, task} =
      PipelineTask.start_link(
        processors: [
          Feline.Turns.UserTurnProcessor,
          {SlowWorker, listener: self()}
        ],
        subscriber: self()
      )

    PipelineTask.queue_frame(task, %TextFrame{text: "bot is saying something long"})
    assert_receive {:working_on, "bot is saying something long", _worker_pid}

    PipelineTask.queue_frame(task, %VADUserStartedSpeakingFrame{})

    assert_receive {:feline_pipeline, :downstream, %InterruptionFrame{}}
    refute_receive {:feline_pipeline, :downstream, %TextFrame{}}, 100
  end
end
