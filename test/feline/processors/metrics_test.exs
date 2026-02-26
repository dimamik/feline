defmodule Feline.Processors.MetricsTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.Processors.Metrics

  alias Feline.Frames.{
    LLMContextFrame,
    LLMRunFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    MetricsFrame,
    TextFrame
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
    {:ok, metrics} = Feline.Processor.Server.start_link({Metrics, []})
    {:ok, collector} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    Processor.link(metrics, collector)
    Process.sleep(50)
    {metrics, collector}
  end

  test "measures LLM TTFB from LLMContextFrame to first LLMTextFrame" do
    {metrics, _collector} = start_pipeline()

    Processor.queue_frame(metrics, %LLMContextFrame{id: make_ref(), context: []}, :downstream)
    Process.sleep(10)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Hello"}, :downstream)

    assert_receive {:collected, %LLMContextFrame{}, :downstream}, 500

    assert_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb, value: ttfb}},
                    :downstream},
                   500

    assert ttfb >= 0
    assert_receive {:collected, %LLMTextFrame{text: "Hello"}, :downstream}, 500
  end

  test "measures LLM TTFB from LLMRunFrame to first LLMTextFrame" do
    {metrics, _collector} = start_pipeline()

    Processor.queue_frame(metrics, %LLMRunFrame{id: make_ref()}, :downstream)
    Process.sleep(10)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Hi"}, :downstream)

    assert_receive {:collected, %LLMRunFrame{}, :downstream}, 500

    assert_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb, value: _}}, :downstream},
                   500

    assert_receive {:collected, %LLMTextFrame{text: "Hi"}, :downstream}, 500
  end

  test "only first LLMTextFrame triggers LLM TTFB metric" do
    {metrics, _collector} = start_pipeline()

    Processor.queue_frame(metrics, %LLMContextFrame{id: make_ref(), context: []}, :downstream)
    Process.sleep(5)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "First"}, :downstream)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Second"}, :downstream)

    assert_receive {:collected, %LLMContextFrame{}, :downstream}, 500

    assert_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb, value: _}}, :downstream},
                   500

    assert_receive {:collected, %LLMTextFrame{text: "First"}, :downstream}, 500
    assert_receive {:collected, %LLMTextFrame{text: "Second"}, :downstream}, 500
    refute_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb}}, :downstream}, 100
  end

  test "measures TTS TTFB from first LLMTextFrame to first TTSAudioRawFrame" do
    {metrics, _collector} = start_pipeline()

    Processor.queue_frame(metrics, %LLMContextFrame{id: make_ref(), context: []}, :downstream)
    Process.sleep(5)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Hello"}, :downstream)
    Process.sleep(10)

    Processor.queue_frame(
      metrics,
      %TTSAudioRawFrame{id: make_ref(), audio: <<0>>, sample_rate: 24_000},
      :downstream
    )

    assert_receive {:collected, %LLMContextFrame{}, :downstream}, 500

    assert_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb, value: _}}, :downstream},
                   500

    assert_receive {:collected, %LLMTextFrame{text: "Hello"}, :downstream}, 500

    assert_receive {:collected, %MetricsFrame{data: %{type: :tts_ttfb, value: ttfb}},
                    :downstream},
                   500

    assert ttfb >= 0
    assert_receive {:collected, %TTSAudioRawFrame{}, :downstream}, 500
  end

  test "only first TTSAudioRawFrame triggers TTS TTFB metric" do
    {metrics, _collector} = start_pipeline()

    Processor.queue_frame(metrics, %LLMContextFrame{id: make_ref(), context: []}, :downstream)
    Process.sleep(5)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Hello"}, :downstream)
    Process.sleep(5)

    Processor.queue_frame(
      metrics,
      %TTSAudioRawFrame{id: make_ref(), audio: <<0>>, sample_rate: 24_000},
      :downstream
    )

    Processor.queue_frame(
      metrics,
      %TTSAudioRawFrame{id: make_ref(), audio: <<1>>, sample_rate: 24_000},
      :downstream
    )

    # Drain expected frames
    assert_receive {:collected, %LLMContextFrame{}, :downstream}, 500
    assert_receive {:collected, %MetricsFrame{data: %{type: :llm_ttfb}}, :downstream}, 500
    assert_receive {:collected, %LLMTextFrame{}, :downstream}, 500
    assert_receive {:collected, %MetricsFrame{data: %{type: :tts_ttfb}}, :downstream}, 500
    assert_receive {:collected, %TTSAudioRawFrame{}, :downstream}, 500
    assert_receive {:collected, %TTSAudioRawFrame{}, :downstream}, 500
    refute_receive {:collected, %MetricsFrame{data: %{type: :tts_ttfb}}, :downstream}, 100
  end

  test "non-tracked frames pass through unchanged" do
    {metrics, _collector} = start_pipeline()

    frame = %TextFrame{id: make_ref(), text: "plain text"}
    Processor.queue_frame(metrics, frame, :downstream)

    assert_receive {:collected, %TextFrame{text: "plain text"}, :downstream}, 500
    refute_receive {:collected, %MetricsFrame{}, :downstream}, 100
  end

  test "emits telemetry events" do
    {metrics, _collector} = start_pipeline()

    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      "test-llm-#{inspect(ref)}",
      [:feline, :metrics, :llm_ttfb],
      fn event, measurements, _metadata, _config ->
        send(test_pid, {:telemetry, event, measurements})
      end,
      nil
    )

    :telemetry.attach(
      "test-tts-#{inspect(ref)}",
      [:feline, :metrics, :tts_ttfb],
      fn event, measurements, _metadata, _config ->
        send(test_pid, {:telemetry, event, measurements})
      end,
      nil
    )

    Processor.queue_frame(metrics, %LLMContextFrame{id: make_ref(), context: []}, :downstream)
    Process.sleep(5)
    Processor.queue_frame(metrics, %LLMTextFrame{id: make_ref(), text: "Hi"}, :downstream)
    Process.sleep(5)

    Processor.queue_frame(
      metrics,
      %TTSAudioRawFrame{id: make_ref(), audio: <<0>>, sample_rate: 24_000},
      :downstream
    )

    assert_receive {:telemetry, [:feline, :metrics, :llm_ttfb], %{duration: _}}, 500
    assert_receive {:telemetry, [:feline, :metrics, :tts_ttfb], %{duration: _}}, 500

    :telemetry.detach("test-llm-#{inspect(ref)}")
    :telemetry.detach("test-tts-#{inspect(ref)}")
  end
end
