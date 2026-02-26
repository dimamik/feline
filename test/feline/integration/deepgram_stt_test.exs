defmodule Feline.Integration.DeepgramSTTTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Feline.Pipeline
  alias Feline.Test.{Collector, IntegrationHelper}
  alias Feline.Frames.{InputAudioRawFrame, TranscriptionFrame}

  setup do
    api_key = IntegrationHelper.require_env!("DEEPGRAM_API_KEY")
    %{api_key: api_key}
  end

  test "transcribes audio via Deepgram API", %{api_key: api_key} do
    # Generate 2 seconds of silence at 16kHz 16-bit mono (64,000 bytes)
    # This won't produce meaningful transcription, but verifies the API round-trip works
    audio = :binary.copy(<<0, 0>>, 32_000)

    pipeline =
      Pipeline.new([
        {Feline.Services.Deepgram.STT,
         api_key: api_key, sample_rate: 16_000, buffer_size: 32_000},
        {Collector, test_pid: self()}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    # Send enough audio to trigger the buffer threshold
    Pipeline.Task.queue_frame(task, %InputAudioRawFrame{
      id: make_ref(),
      audio: audio,
      sample_rate: 16_000
    })

    # We should get the audio frame passed through + a TranscriptionFrame
    assert_receive {:frame, %InputAudioRawFrame{}}, 15_000
    assert_receive {:frame, %TranscriptionFrame{text: text}}, 15_000
    assert is_binary(text)

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end
end
