defmodule Feline.ServicesTest do
  use ExUnit.Case, async: true

  alias Feline.FakeServers
  alias Feline.FakeServers.{FakeCartesia, FakeDeepgram, FakeOpenAI, WSUpgrade}
  alias Feline.Pipeline.Task, as: PipelineTask
  alias Feline.Processors.ContextAggregator

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TextFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSTextFrame
  }

  describe "OpenAI LLM" do
    test "streams text deltas bracketed by response start/end" do
      {_server, port} = FakeServers.start_http(FakeOpenAI)

      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {Feline.Services.OpenAI.LLM, base_url: "http://localhost:#{port}", api_key: "test"}
          ],
          subscriber: self()
        )

      context = Feline.LLM.Context.new(messages: [%{"role" => "user", "content" => "hi"}])
      PipelineTask.queue_frame(task, %LLMContextFrame{context: context})

      assert_receive {:feline_pipeline, :downstream, %LLMFullResponseStartFrame{}}
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "Hello "}}
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "from the "}}
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "fake LLM. "}}
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "Bye!"}}
      assert_receive {:feline_pipeline, :downstream, %LLMFullResponseEndFrame{}}
    end

    test "tool call round-trip: LLM calls tool, context re-runs, final answer streams" do
      {_server, port} = FakeServers.start_http(FakeOpenAI)
      listener = self()

      tools = [
        %{
          name: "get_weather",
          description: "weather",
          parameters: %{type: "object", properties: %{city: %{type: "string"}}},
          handler: fn arguments ->
            send(listener, {:tool_called, arguments})
            %{"temp" => 21}
          end
        }
      ]

      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {ContextAggregator, tools: tools},
            {Feline.Services.OpenAI.LLM,
             base_url: "http://localhost:#{port}", api_key: "test", tools: tools}
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "weather in Krakow?"})

      assert_receive {:tool_called, %{"city" => "Krakow"}}
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "It is "}}, 1_000
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "21 degrees. "}}
    end

    test "a raising tool handler becomes an error result, not a crash" do
      {_server, port} = FakeServers.start_http(FakeOpenAI)

      tools = [
        %{
          name: "get_weather",
          description: "weather",
          parameters: %{},
          handler: fn _arguments -> raise "boom" end
        }
      ]

      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {ContextAggregator, tools: tools},
            {Feline.Services.OpenAI.LLM,
             base_url: "http://localhost:#{port}", api_key: "test", tools: tools}
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "weather?"})

      # The tool errored, but the pipeline survives and the LLM re-runs with
      # the error result as tool output.
      assert_receive {:feline_pipeline, :downstream, %LLMTextFrame{text: "It is "}}, 1_000
    end
  end

  describe "Deepgram STT" do
    test "audio in, interim + final transcription out" do
      {_server, port} = FakeServers.start_http({WSUpgrade, FakeDeepgram})

      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {Feline.Services.Deepgram.STT, url: "ws://localhost:#{port}/v1/listen"}
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %InputAudioRawFrame{
        audio: :binary.copy(<<100::little-signed-16>>, 160)
      })

      assert_receive {:feline_pipeline, :downstream, %InterimTranscriptionFrame{text: "hello"}},
                     1_000

      assert_receive {:feline_pipeline, :downstream, %TranscriptionFrame{text: "hello world."}}
    end
  end

  describe "ElevenLabs TTS" do
    test "each sentence synthesizes in order: started/text/audio/stopped" do
      {_server, port} = FakeServers.start_http(FakeServers.FakeElevenLabs)

      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {Feline.Services.ElevenLabs.TTS, base_url: "http://localhost:#{port}", api_key: "k"}
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %TextFrame{text: "First sentence."})
      PipelineTask.queue_frame(task, %TextFrame{text: "Second sentence."})

      assert_receive {:feline_pipeline, :downstream, %TTSStartedFrame{}}, 1_000
      assert_receive {:feline_pipeline, :downstream, %TTSTextFrame{text: "First sentence."}}
      assert_receive {:feline_pipeline, :downstream, %TTSAudioRawFrame{audio: audio}}, 1_000
      assert byte_size(audio) == 20
      assert_receive {:feline_pipeline, :downstream, %TTSStoppedFrame{}}, 1_000

      assert_receive {:feline_pipeline, :downstream, %TTSTextFrame{text: "Second sentence."}},
                     1_000

      assert_receive {:feline_pipeline, :downstream, %TTSAudioRawFrame{}}, 1_000
    end
  end

  describe "Cartesia TTS" do
    test "sentence in, started/text/audio/stopped out, sequential contexts" do
      {_server, port} = FakeServers.start_http({WSUpgrade, FakeCartesia})

      {:ok, task} =
        PipelineTask.start_link(
          processors: [{Feline.Services.Cartesia.TTS, url: "ws://localhost:#{port}/tts"}],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %TextFrame{text: "First sentence."})
      PipelineTask.queue_frame(task, %TextFrame{text: "Second sentence."})

      assert_receive {:feline_pipeline, :downstream, %TTSStartedFrame{}}, 1_000
      assert_receive {:feline_pipeline, :downstream, %TTSTextFrame{text: "First sentence."}}
      assert_receive {:feline_pipeline, :downstream, %TTSAudioRawFrame{audio: audio}}
      assert byte_size(audio) == 20
      assert_receive {:feline_pipeline, :downstream, %TTSStoppedFrame{}}

      assert_receive {:feline_pipeline, :downstream, %TTSTextFrame{text: "Second sentence."}},
                     1_000

      assert_receive {:feline_pipeline, :downstream, %TTSAudioRawFrame{}}
    end
  end
end
