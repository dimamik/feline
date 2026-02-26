defmodule Feline.Pipeline.Sink do
  @moduledoc """
  Exit point of a pipeline. Downstream frames are forwarded to the pipeline
  task. Upstream frames pass through to the last user processor.
  """
  use Feline.Processor

  alias Feline.Frames.{InterruptionFrame, InterruptionCompletionFrame}

  @impl true
  def init(opts) do
    {:ok, %{task_pid: Keyword.fetch!(opts, :task_pid)}}
  end

  @impl true
  def handle_frame(
        %InterruptionFrame{ref: ref, caller: caller} = frame,
        :downstream,
        push_fn,
        state
      )
      when ref != nil and caller != nil do
    send(state.task_pid, {:downstream_frame, frame})
    send(caller, {:interruption_complete, ref})
    push_fn.(%InterruptionCompletionFrame{id: make_ref(), ref: ref}, :upstream)
    {:ok, state}
  end

  def handle_frame(frame, :downstream, _push_fn, state) do
    send(state.task_pid, {:downstream_frame, frame})
    {:ok, state}
  end

  def handle_frame(frame, :upstream, _push_fn, state) do
    {:push, frame, :upstream, state}
  end
end
