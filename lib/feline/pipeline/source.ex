defmodule Feline.Pipeline.Source do
  @moduledoc """
  Entry point of a pipeline. Downstream frames pass through to the first
  processor. Upstream frames are forwarded to the pipeline task.
  """
  use Feline.Processor

  @impl true
  def init(opts) do
    {:ok, %{task_pid: Keyword.fetch!(opts, :task_pid)}}
  end

  @impl true
  def handle_frame(frame, :downstream, _push_fn, state) do
    {:push, frame, :downstream, state}
  end

  def handle_frame(frame, :upstream, _push_fn, state) do
    send(state.task_pid, {:upstream_frame, frame})
    {:ok, state}
  end
end
