defmodule Feline.PipelineTest do
  use ExUnit.Case, async: true

  alias Feline.Pipeline.Task, as: PipelineTask
  alias Feline.TestProcessors.{CleanupReporter, Crasher, Recorder, SlowWorker}

  alias Feline.Frames.All.{
    CancelFrame,
    EndFrame,
    FunctionCallResultFrame,
    InterruptionFrame,
    StartFrame,
    TextFrame
  }

  defp start_pipeline(processors, opts \\ []) do
    {:ok, task} =
      PipelineTask.start_link(
        [processors: processors, subscriber: self()] ++ opts
      )

    task
  end

  test "frames flow downstream in order and reach the sink" do
    task = start_pipeline([{Recorder, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "one"})
    PipelineTask.queue_frame(task, %TextFrame{text: "two"})

    assert_receive {:recorded, :recorder, %TextFrame{text: "one"}, :downstream}
    assert_receive {:recorded, :recorder, %TextFrame{text: "two"}, :downstream}
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "one"}}
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "two"}}
  end

  test "StartFrame runs handle_setup before any other frame and carries params" do
    defmodule SetupRecorder do
      use Feline.Processor

      @impl true
      def handle_setup(start_frame, _ctx, %{listener: listener} = state) do
        send(listener, {:setup, start_frame})
        {:ok, state}
      end
    end

    task =
      start_pipeline([{SetupRecorder, listener: self()}], params: [audio_out_sample_rate: 8_000])

    PipelineTask.queue_frame(task, %TextFrame{text: "after start"})

    assert_receive {:setup, %StartFrame{audio_out_sample_rate: 8_000}}
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "after start"}}
  end

  test "EndFrame drains the pipeline then stops the task normally" do
    task = start_pipeline([{CleanupReporter, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "last words"})
    PipelineTask.stop_when_done(task)

    assert PipelineTask.await(task, 2_000) == :ok
    assert_receive :cleaned_up
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "last words"}}
  end

  test "CancelFrame stops the pipeline immediately" do
    task = start_pipeline([])
    PipelineTask.cancel(task)
    assert PipelineTask.await(task, 2_000) == :ok
  end

  test "a processor crash takes the pipeline down" do
    task = start_pipeline([Crasher])
    Process.flag(:trap_exit, true)

    PipelineTask.queue_frame(task, %TextFrame{text: "boom"})

    assert {:error, {%RuntimeError{message: "boom"}, _stacktrace}} =
             PipelineTask.await(task, 2_000)
  end

  test "data frames queue behind an async task and flow in order after it finishes" do
    task = start_pipeline([{SlowWorker, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "a"})
    PipelineTask.queue_frame(task, %TextFrame{text: "b"})

    assert_receive {:working_on, "a", worker_pid}
    refute_receive {:feline_pipeline, :downstream, %TextFrame{}}, 50

    send(worker_pid, :finish)
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "a!done"}}

    assert_receive {:working_on, "b", second_worker}
    send(second_worker, :finish)
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "b!done"}}
  end

  test "interruption kills the in-flight task and flushes queued data frames" do
    task = start_pipeline([{SlowWorker, listener: self()}, {Recorder, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "a"})
    PipelineTask.queue_frame(task, %TextFrame{text: "b"})
    assert_receive {:working_on, "a", _worker_pid}

    PipelineTask.queue_frame(task, %InterruptionFrame{})

    # The interruption itself propagates; the interrupted/flushed text never does.
    assert_receive {:recorded, :recorder, %InterruptionFrame{}, :downstream}
    refute_receive {:working_on, "b", _}, 100
    refute_receive {:feline_pipeline, :downstream, %TextFrame{}}, 50

    # Pipeline still alive and processing new frames.
    PipelineTask.queue_frame(task, %TextFrame{text: "c"})
    assert_receive {:working_on, "c", worker_pid}
    send(worker_pid, :finish)
    assert_receive {:feline_pipeline, :downstream, %TextFrame{text: "c!done"}}
  end

  test "interruption preserves uninterruptible frames in the pending queue" do
    task = start_pipeline([{SlowWorker, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "a"})
    assert_receive {:working_on, "a", _worker_pid}

    result = %FunctionCallResultFrame{function_name: "f", tool_call_id: "1", result: "42"}
    PipelineTask.queue_frame(task, result)
    PipelineTask.queue_frame(task, %TextFrame{text: "flushed"})
    PipelineTask.queue_frame(task, %InterruptionFrame{})

    assert_receive {:feline_pipeline, :downstream, %FunctionCallResultFrame{result: "42"}}
    refute_receive {:feline_pipeline, :downstream, %TextFrame{}}, 50
  end

  test "EndFrame survives interruption (uninterruptible)" do
    task = start_pipeline([{SlowWorker, listener: self()}])

    PipelineTask.queue_frame(task, %TextFrame{text: "a"})
    assert_receive {:working_on, "a", _worker_pid}

    PipelineTask.queue_frame(task, %EndFrame{})
    PipelineTask.queue_frame(task, %InterruptionFrame{})

    assert PipelineTask.await(task, 2_000) == :ok
  end

  test "upstream frames reach the source and the subscriber" do
    defmodule UpstreamPusher do
      use Feline.Processor

      alias Feline.Frames.All.TextFrame

      @impl true
      def handle_frame(%TextFrame{text: "ping"}, :downstream, _ctx, state) do
        {:push, %TextFrame{text: "pong"}, :upstream, state}
      end

      def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
    end

    task = start_pipeline([UpstreamPusher])
    PipelineTask.queue_frame(task, %TextFrame{text: "ping"})

    assert_receive {:feline_pipeline, :upstream, %TextFrame{text: "pong"}}
  end
end
