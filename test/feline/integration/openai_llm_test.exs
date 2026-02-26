defmodule Feline.Integration.OpenAILLMTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Feline.Pipeline
  alias Feline.Context
  alias Feline.Test.{Collector, IntegrationHelper}

  alias Feline.Frames.{
    LLMContextFrame,
    LLMTextFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame
  }

  setup do
    api_key = IntegrationHelper.require_env!("OPENAI_API_KEY")
    %{api_key: api_key}
  end

  test "non-streaming LLM returns a text response", %{api_key: api_key} do
    context =
      Context.new([
        %{"role" => "user", "content" => "Say the word 'pineapple' and nothing else."}
      ])

    pipeline =
      Pipeline.new([
        {Feline.Services.OpenAI.LLM, api_key: api_key, model: "gpt-4.1-mini"},
        {Collector, test_pid: self()}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %LLMContextFrame{id: make_ref(), context: context})

    assert_receive {:frame, %LLMFullResponseStartFrame{}}, 15_000
    assert_receive {:frame, %LLMTextFrame{text: text}}, 15_000
    assert is_binary(text) and text != ""
    assert_receive {:frame, %LLMFullResponseEndFrame{}}, 15_000

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end

  test "streaming LLM returns chunked text responses", %{api_key: api_key} do
    context =
      Context.new([
        %{"role" => "user", "content" => "Count from 1 to 5, one number per line."}
      ])

    pipeline =
      Pipeline.new([
        {Feline.Services.OpenAI.StreamingLLM, api_key: api_key, model: "gpt-4.1-mini"},
        {Collector, test_pid: self()}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %LLMContextFrame{id: make_ref(), context: context})

    assert_receive {:frame, %LLMFullResponseStartFrame{}}, 15_000

    # Collect all text frames until we get the end frame
    text_frames = collect_until_end([], 15_000)

    assert length(text_frames) > 1,
           "Expected multiple streaming chunks, got #{length(text_frames)}"

    full_text = Enum.map_join(text_frames, "", & &1.text)
    assert full_text =~ "1"

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end

  defp collect_until_end(acc, timeout) do
    receive do
      {:frame, %LLMTextFrame{} = frame} ->
        collect_until_end([frame | acc], timeout)

      {:frame, %LLMFullResponseEndFrame{}} ->
        Enum.reverse(acc)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end
