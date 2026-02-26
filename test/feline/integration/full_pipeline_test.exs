defmodule Feline.Integration.FullPipelineTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Feline.Pipeline
  alias Feline.Context
  alias Feline.Test.{Collector, IntegrationHelper}
  alias Feline.Processors.SentenceAggregator

  alias Feline.Frames.{
    LLMContextFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame
  }

  setup do
    openai_key = IntegrationHelper.require_env!("OPENAI_API_KEY")
    elevenlabs_key = IntegrationHelper.require_env!("ELEVENLABS_API_KEY")
    voice_id = IntegrationHelper.require_env!("ELEVENLABS_VOICE_ID")

    %{openai_key: openai_key, elevenlabs_key: elevenlabs_key, voice_id: voice_id}
  end

  test "LLM → SentenceAggregator → TTS full pipeline", ctx do
    context =
      Context.new([
        %{"role" => "user", "content" => "Say exactly: Hello there!"}
      ])

    pipeline =
      Pipeline.new([
        {Feline.Services.OpenAI.LLM, api_key: ctx.openai_key, model: "gpt-4.1-mini"},
        {SentenceAggregator, []},
        {Feline.Services.ElevenLabs.TTS,
         api_key: ctx.elevenlabs_key, voice_id: ctx.voice_id, sample_rate: 24_000},
        {Collector, test_pid: self()}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %LLMContextFrame{id: make_ref(), context: context})

    assert_receive {:frame, %TTSStartedFrame{}}, 30_000
    assert_receive {:frame, %TTSAudioRawFrame{audio: audio}}, 30_000
    assert byte_size(audio) > 0
    assert_receive {:frame, %TTSStoppedFrame{}}, 30_000

    IntegrationHelper.play_pcm!(audio, 24_000)

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end
end
