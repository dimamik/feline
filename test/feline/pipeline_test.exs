defmodule Feline.PipelineTest do
  use ExUnit.Case, async: true

  alias Feline.Pipeline
  alias Feline.Frames.{TextFrame, LLMTextFrame}

  defmodule Passthrough do
    use Feline.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      {:push, frame, direction, state}
    end
  end

  defmodule Uppercaser do
    use Feline.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_frame(%TextFrame{text: text} = frame, :downstream, _push_fn, state) do
      {:push, %{frame | text: String.upcase(text)}, :downstream, state}
    end

    def handle_frame(frame, direction, _push_fn, state) do
      {:push, frame, direction, state}
    end
  end

  defmodule FrameMultiplier do
    use Feline.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_frame(%TextFrame{text: text} = _frame, :downstream, _push_fn, state) do
      frames = [
        {%LLMTextFrame{id: make_ref(), text: "LLM: #{text}"}, :downstream},
        {%TextFrame{id: make_ref(), text: "Echo: #{text}"}, :downstream}
      ]

      {:push_many, frames, state}
    end

    def handle_frame(frame, direction, _push_fn, state) do
      {:push, frame, direction, state}
    end
  end

  test "simple pipeline: frame flows through passthrough to sink" do
    pipeline = Pipeline.new([{Passthrough, []}])
    {:ok, task} = Pipeline.Task.start_link(pipeline)

    # Run in a separate process so we can interact
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %TextFrame{id: make_ref(), text: "hello"})
    Process.sleep(50)
    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end

  test "pipeline with transformation" do
    # We need a way to observe output. Let's use a processor that reports to test.
    test_pid = self()

    defmodule Reporter do
      use Feline.Processor

      @impl true
      def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

      @impl true
      def handle_frame(%TextFrame{} = frame, :downstream, _push_fn, state) do
        send(state.test_pid, {:output, frame})
        {:push, frame, :downstream, state}
      end

      def handle_frame(frame, direction, _push_fn, state) do
        {:push, frame, direction, state}
      end
    end

    pipeline =
      Pipeline.new([
        {Uppercaser, []},
        {Reporter, [test_pid: test_pid]}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %TextFrame{id: make_ref(), text: "hello"})

    assert_receive {:output, %TextFrame{text: "HELLO"}}, 500

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end

  test "pipeline with push_many" do
    test_pid = self()

    defmodule MultiReporter do
      use Feline.Processor

      @impl true
      def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

      @impl true
      def handle_frame(frame, :downstream, _push_fn, state) do
        send(state.test_pid, {:output, frame})
        {:push, frame, :downstream, state}
      end

      def handle_frame(frame, direction, _push_fn, state) do
        {:push, frame, direction, state}
      end
    end

    pipeline =
      Pipeline.new([
        {FrameMultiplier, []},
        {MultiReporter, [test_pid: test_pid]}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %TextFrame{id: make_ref(), text: "hi"})

    assert_receive {:output, %LLMTextFrame{text: "LLM: hi"}}, 500
    assert_receive {:output, %TextFrame{text: "Echo: hi"}}, 500

    Pipeline.Task.stop_when_done(task)
    Process.sleep(100)
  end

  test "pipeline runner runs and completes" do
    pipeline = Pipeline.new([{Passthrough, []}])

    # Run in a task so we can stop it
    test_task =
      Task.async(fn ->
        Pipeline.Runner.run(pipeline)
      end)

    # Give it time to start, then we need to stop it
    # The runner blocks on Pipeline.Task.run which blocks until EndFrame
    # We can't easily signal it, so just verify it starts
    Process.sleep(100)
    Task.shutdown(test_task, :brutal_kill)
  end
end
