defmodule Feline.Pipeline.Runner do
  @moduledoc """
  Top-level entry point for running a pipeline. Starts a Pipeline.Task,
  waits for completion, and handles graceful shutdown.
  """

  alias Feline.Pipeline

  def run(%Pipeline{} = pipeline, opts \\ []) do
    {:ok, task} = Pipeline.Task.start_link(pipeline, opts)

    ref = Process.monitor(task)
    result = Pipeline.Task.run(task)

    receive do
      {:DOWN, ^ref, :process, ^task, _reason} -> :ok
    after
      100 -> :ok
    end

    result
  end
end
