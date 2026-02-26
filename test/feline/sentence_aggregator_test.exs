defmodule Feline.Processors.SentenceAggregatorTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.Processors.SentenceAggregator

  alias Feline.Frames.{
    LLMTextFrame,
    TextFrame,
    LLMFullResponseEndFrame,
    InterruptionFrame,
    EndFrame,
    InputAudioRawFrame
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

  defp start_pipeline do
    {:ok, aggregator} = Feline.Processor.Server.start_link({SentenceAggregator, []})
    {:ok, collector} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    Processor.link(aggregator, collector)
    Process.sleep(50)
    {aggregator, collector}
  end

  defp send_llm_text(aggregator, text) do
    Processor.queue_frame(aggregator, %LLMTextFrame{id: make_ref(), text: text}, :downstream)
  end

  test "buffers tokens until sentence end" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "Hello")
    send_llm_text(aggregator, " world")
    send_llm_text(aggregator, ".")

    assert_receive {:collected, %TextFrame{text: "Hello world."}, :downstream}, 500
  end

  test "emits at first sentence boundary" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "First. Second")

    assert_receive {:collected, %TextFrame{text: "First."}, :downstream}, 500
    refute_receive {:collected, %TextFrame{text: "Second"}, :downstream}, 100
  end

  test "handles exclamation marks" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "Wow!")

    assert_receive {:collected, %TextFrame{text: "Wow!"}, :downstream}, 500
  end

  test "handles question marks" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "Really?")

    assert_receive {:collected, %TextFrame{text: "Really?"}, :downstream}, 500
  end

  test "flushes buffer on LLMFullResponseEndFrame" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "incomplete text")

    Processor.queue_frame(
      aggregator,
      %LLMFullResponseEndFrame{id: make_ref()},
      :downstream
    )

    assert_receive {:collected, %TextFrame{text: "incomplete text"}, :downstream}, 500
    assert_receive {:collected, %LLMFullResponseEndFrame{}, :downstream}, 500
  end

  test "clears buffer on InterruptionFrame" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "buffered text")

    Processor.queue_frame(
      aggregator,
      %InterruptionFrame{id: make_ref()},
      :downstream
    )

    assert_receive {:collected, %InterruptionFrame{}, :downstream}, 500
    refute_receive {:collected, %TextFrame{}, :downstream}, 100
  end

  test "flushes buffer on EndFrame" do
    {aggregator, _collector} = start_pipeline()

    send_llm_text(aggregator, "remaining text")

    Processor.queue_frame(
      aggregator,
      %EndFrame{id: make_ref()},
      :downstream
    )

    assert_receive {:collected, %TextFrame{text: "remaining text"}, :downstream}, 500
    assert_receive {:collected, %EndFrame{}, :downstream}, 500
  end

  test "passes through non-text frames unchanged" do
    {aggregator, _collector} = start_pipeline()

    frame = %InputAudioRawFrame{id: make_ref(), audio: <<0, 1, 2>>, sample_rate: 16_000}
    Processor.queue_frame(aggregator, frame, :downstream)

    assert_receive {:collected, %InputAudioRawFrame{audio: <<0, 1, 2>>, sample_rate: 16_000},
                    :downstream},
                   500
  end
end
