defmodule Feline.Integration.ElevenLabsTTSTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Feline.Pipeline
  alias Feline.Test.{Collector, IntegrationHelper}

  alias Feline.Frames.{
    TextFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame
  }

  setup do
    api_key = IntegrationHelper.require_env!("ELEVENLABS_API_KEY")
    voice_id = IntegrationHelper.require_env!("ELEVENLABS_VOICE_ID")
    %{api_key: api_key, voice_id: voice_id}
  end

  test "synthesizes speech via ElevenLabs API", %{api_key: api_key, voice_id: voice_id} do
    pipeline =
      Pipeline.new([
        {Feline.Services.ElevenLabs.TTS,
         api_key: api_key, voice_id: voice_id, sample_rate: 24_000},
        {Collector, test_pid: self()}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %TextFrame{id: make_ref(), text: "Hello world"})

    assert_receive {:frame, %TTSStartedFrame{}}, 15_000
    assert_receive {:frame, %TTSAudioRawFrame{audio: audio}}, 15_000
    assert is_binary(audio) and byte_size(audio) > 0
    assert_receive {:frame, %TTSStoppedFrame{}}, 15_000

    IntegrationHelper.play_pcm!(audio, 24_000)

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end
end
