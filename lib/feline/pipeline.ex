defmodule Feline.Pipeline do
  @moduledoc """
  A pipeline spec is a list of processors: `module` or `{module, opts}`.
  Instantiated and run by `Feline.Pipeline.Task`.
  """

  def normalize(processors) do
    Enum.map(processors, fn
      {module, opts} -> {module, opts}
      module when is_atom(module) -> {module, []}
    end)
  end
end

defmodule Feline.Pipeline.Source do
  @moduledoc "Head bookend: relays upstream frames out of the pipeline to the task."
  use Feline.Processor

  @impl true
  def handle_frame(frame, :upstream, _ctx, %{task: task} = state) do
    send(task, {:feline_pipeline, :upstream, frame})
    {:ok, state}
  end

  def handle_frame(frame, :downstream, _ctx, state), do: {:push, frame, :downstream, state}
end

defmodule Feline.Pipeline.Sink do
  @moduledoc "Tail bookend: relays downstream frames out of the pipeline to the task."
  use Feline.Processor

  @impl true
  def handle_frame(frame, :downstream, _ctx, %{task: task} = state) do
    send(task, {:feline_pipeline, :downstream, frame})
    {:ok, state}
  end

  def handle_frame(frame, :upstream, _ctx, state), do: {:push, frame, :upstream, state}
end
