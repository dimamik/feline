defmodule Feline.Pipeline.Parallel do
  @moduledoc """
  A processor that runs N pipeline branches concurrently.

  System frames are broadcast to all branches. Data frames are sent to
  all branches (fan-out). Output from all branches is merged into the
  single downstream.

  Usage in a pipeline:

      pipeline = Feline.Pipeline.new([
        {SomeProcessor, []},
        {Feline.Pipeline.Parallel, branches: [
          [{BranchA1, opts}, {BranchA2, opts}],
          [{BranchB1, opts}, {BranchB2, opts}]
        ]},
        {AnotherProcessor, []}
      ])
  """
  use Feline.Processor

  alias Feline.Processor

  @impl Feline.Processor
  def init(opts) do
    branch_specs = Keyword.fetch!(opts, :branches)
    {:ok, %{branch_specs: branch_specs, branches: [], started: false}}
  end

  @impl Feline.Processor
  def handle_frame(frame, direction, push_fn, state) do
    state =
      if state.started do
        state
      else
        start_branches(state, push_fn)
      end

    # Fan out to all branch heads
    for {head, _all_pids} <- state.branches do
      Processor.queue_frame(head, frame, direction)
    end

    {:ok, state}
  end

  @impl Feline.Processor
  def handle_info({:branch_output, frame, direction}, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_info(_msg, _push_fn, state) do
    {:ok, state}
  end

  defp start_branches(state, _push_fn) do
    owner = self()

    branches =
      Enum.map(state.branch_specs, fn specs ->
        # Start each processor in the branch
        pids =
          Enum.map(specs, fn {mod, opts} ->
            {:ok, pid} = Processor.Server.start_link({mod, opts})
            pid
          end)

        # Add a collector at the end that forwards output back to us
        {:ok, collector} =
          Processor.Server.start_link({Feline.Pipeline.Parallel.Collector, [owner: owner]})

        all = pids ++ [collector]

        # Link processors in chain
        all
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.each(fn [a, b] -> Processor.link(a, b) end)

        # Monitor all
        for pid <- all, do: Process.monitor(pid)

        {List.first(all), all}
      end)

    %{state | branches: branches, started: true}
  end
end

defmodule Feline.Pipeline.Parallel.Collector do
  @moduledoc false
  use Feline.Processor

  @impl Feline.Processor
  def init(opts) do
    {:ok, %{owner: Keyword.fetch!(opts, :owner)}}
  end

  @impl Feline.Processor
  def handle_frame(frame, direction, _push_fn, state) do
    send(state.owner, {:branch_output, frame, direction})
    {:ok, state}
  end
end
