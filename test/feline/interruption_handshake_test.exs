defmodule Feline.InterruptionHandshakeTest do
  use ExUnit.Case, async: true

  alias Feline.Pipeline

  alias Feline.Frames.{
    TextFrame,
    InterruptionFrame,
    InterruptionCompletionFrame
  }

  defmodule Collector do
    use Feline.Processor

    @impl true
    def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      send(state.test_pid, {:collected, frame, direction})
      {:push, frame, direction, state}
    end
  end

  test "interruption without ref/caller works as before (no ack)" do
    pipeline = Pipeline.new([{Collector, [test_pid: self()]}])
    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %InterruptionFrame{id: make_ref()})

    assert_receive {:collected, %InterruptionFrame{ref: nil, caller: nil}, :downstream}, 500
    refute_receive {:interruption_complete, _}, 100
  end

  test "interruption with ref/caller sends ack to caller" do
    pipeline = Pipeline.new([{Collector, [test_pid: self()]}])
    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    ref = make_ref()

    Pipeline.Task.queue_frame(
      task,
      %InterruptionFrame{id: make_ref(), ref: ref, caller: self()}
    )

    assert_receive {:interruption_complete, ^ref}, 500
  end

  test "sink pushes InterruptionCompletionFrame upstream after handshake" do
    pipeline = Pipeline.new([{Collector, [test_pid: self()]}])
    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    ref = make_ref()

    Pipeline.Task.queue_frame(
      task,
      %InterruptionFrame{id: make_ref(), ref: ref, caller: self()}
    )

    assert_receive {:collected, %InterruptionFrame{ref: ^ref}, :downstream}, 500
    assert_receive {:collected, %InterruptionCompletionFrame{ref: ^ref}, :upstream}, 500
  end

  test "buffered frames are cleared on interruption" do
    pipeline = Pipeline.new([{Collector, [test_pid: self()]}])
    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn(fn -> Pipeline.Task.run(task) end)
    Process.sleep(50)

    Pipeline.Task.queue_frame(task, %TextFrame{id: make_ref(), text: "hello"})
    Process.sleep(20)

    Pipeline.Task.queue_frame(
      task,
      %InterruptionFrame{id: make_ref(), ref: make_ref(), caller: self()}
    )

    assert_receive {:collected, %TextFrame{text: "hello"}, :downstream}, 500
    assert_receive {:interruption_complete, _}, 500
  end
end
