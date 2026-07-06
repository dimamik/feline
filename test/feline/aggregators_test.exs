defmodule Feline.AggregatorsTest do
  use ExUnit.Case, async: true

  alias Feline.Pipeline.Task, as: PipelineTask
  alias Feline.Processors.{AssistantCollector, ContextAggregator, SentenceAggregator}

  alias Feline.Frames.All.{
    FunctionCallResultFrame,
    InterruptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMMessagesAppendFrame,
    LLMTextFrame,
    TextFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame
  }

  describe "SentenceAggregator" do
    setup do
      {:ok, task} =
        PipelineTask.start_link(processors: [SentenceAggregator], subscriber: self())

      %{task: task}
    end

    test "extracts every complete sentence from a chunk, not just the first", %{task: task} do
      PipelineTask.queue_frame(task, %LLMTextFrame{text: "One. Two! Three? Four is not do"})

      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "One."}}
      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "Two!"}}
      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "Three?"}}
      refute_receive {:feline_pipeline, :downstream, %TextFrame{text: "Four" <> _rest}}, 50

      PipelineTask.queue_frame(task, %LLMTextFrame{text: "ne yet."})
      PipelineTask.queue_frame(task, %LLMTextFrame{text: " Fin"})
      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "Four is not done yet."}}

      PipelineTask.queue_frame(task, %LLMFullResponseEndFrame{})
      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "Fin"}}
    end

    test "interruption clears the buffer", %{task: task} do
      PipelineTask.queue_frame(task, %LLMTextFrame{text: "Partial sente"})
      PipelineTask.queue_frame(task, %InterruptionFrame{})
      PipelineTask.queue_frame(task, %LLMFullResponseEndFrame{})

      refute_receive {:feline_pipeline, :downstream, %TextFrame{}}, 50
    end
  end

  describe "ContextAggregator" do
    test "commits the user turn on stop-speaking and runs the LLM" do
      {:ok, task} =
        PipelineTask.start_link(
          processors: [{ContextAggregator, system_prompt: "be brief"}],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %UserStartedSpeakingFrame{})
      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "hello"})
      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "there"})
      PipelineTask.queue_frame(task, %UserStoppedSpeakingFrame{})

      assert_receive {:feline_pipeline, :downstream, %LLMContextFrame{context: context}}

      assert context.messages == [
               %{"role" => "system", "content" => "be brief"},
               %{"role" => "user", "content" => "hello there"}
             ]
    end

    test "a late final transcription after stop-speaking still commits" do
      {:ok, task} = PipelineTask.start_link(processors: [ContextAggregator], subscriber: self())

      PipelineTask.queue_frame(task, %UserStartedSpeakingFrame{})
      PipelineTask.queue_frame(task, %UserStoppedSpeakingFrame{})
      refute_receive {:feline_pipeline, :downstream, %LLMContextFrame{}}, 50

      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "late words"})

      assert_receive {:feline_pipeline, :downstream, %LLMContextFrame{context: context}}
      assert [%{"role" => "user", "content" => "late words"}] = context.messages
    end

    test "assistant commits flow upstream into the context" do
      {:ok, task} =
        PipelineTask.start_link(
          processors: [ContextAggregator, AssistantCollector],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %LLMFullResponseStartFrame{})
      PipelineTask.queue_frame(task, %LLMTextFrame{text: "Hi "})
      PipelineTask.queue_frame(task, %LLMTextFrame{text: "there!"})
      PipelineTask.queue_frame(task, %LLMFullResponseEndFrame{})
      assert_receive {:feline_pipeline, :downstream, %LLMFullResponseEndFrame{}}

      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "next question"})
      assert_receive {:feline_pipeline, :downstream, %LLMContextFrame{context: context}}

      assert context.messages == [
               %{"role" => "assistant", "content" => "Hi there!"},
               %{"role" => "user", "content" => "next question"}
             ]
    end

    test "function call results append the tool exchange and re-run the LLM" do
      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            ContextAggregator,
            {Feline.TestProcessors.UpstreamReflector, reflect: FunctionCallResultFrame}
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %TranscriptionFrame{text: "weather?"})
      assert_receive {:feline_pipeline, :downstream, %LLMContextFrame{}}

      result = %FunctionCallResultFrame{
        function_name: "get_weather",
        tool_call_id: "call_1",
        arguments: %{"city" => "Krakow"},
        result: %{"temp" => 21}
      }

      PipelineTask.queue_frame(task, result)

      assert_receive {:feline_pipeline, :downstream, %LLMContextFrame{context: context}}

      assert [
               %{"role" => "user", "content" => "weather?"},
               %{"role" => "assistant", "tool_calls" => [tool_call]},
               %{"role" => "tool", "tool_call_id" => "call_1", "content" => tool_content}
             ] = context.messages

      assert tool_call["function"]["name"] == "get_weather"
      assert Jason.decode!(tool_content) == %{"temp" => 21}
    end

    test "greeting is committed to context and pushed as speakable text" do
      {:ok, _task} =
        PipelineTask.start_link(
          processors: [{ContextAggregator, greeting: "Hello!"}],
          subscriber: self()
        )

      assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "Hello!"}}
    end
  end

  describe "AssistantCollector" do
    test "interruption commits the partial response upstream" do
      {:ok, task} =
        PipelineTask.start_link(
          processors: [
            {Feline.TestProcessors.Recorder, listener: self(), tag: :above},
            AssistantCollector
          ],
          subscriber: self()
        )

      PipelineTask.queue_frame(task, %LLMFullResponseStartFrame{})
      PipelineTask.queue_frame(task, %LLMTextFrame{text: "I was saying"})
      PipelineTask.queue_frame(task, %InterruptionFrame{})

      assert_receive {:recorded, :above, %LLMMessagesAppendFrame{messages: messages}, :upstream}
      assert messages == [%{"role" => "assistant", "content" => "I was saying"}]
    end
  end
end
