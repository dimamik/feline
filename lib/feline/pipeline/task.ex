defmodule Feline.Pipeline.Task do
  @moduledoc """
  Instantiates and runs a pipeline: starts one `Feline.Processor.Server` per
  processor (plus Source/Sink bookends), links them, injects `StartFrame`,
  and shuts everything down when `EndFrame`/`CancelFrame` reaches the sink.

  All processor servers are linked to this GenServer: if any processor
  crashes, the whole pipeline goes down with it.

  Options:

    * `:processors` (required) - list of `module` or `{module, opts}`
    * `:params` - `StartFrame` overrides, e.g. `audio_out_sample_rate: 24_000`
    * `:subscriber` - pid receiving `{:feline_pipeline, direction, frame}`
      for frames that exit the pipeline at either end (used by transports-less
      pipelines and tests)
  """

  use GenServer

  alias Feline.Pipeline
  alias Feline.Processor.Server

  alias Feline.Frames.All.{
    CancelFrame,
    EndFrame,
    ErrorFrame,
    StartFrame
  }

  require Logger

  # -- API --

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def queue_frame(task, frame, direction \\ :downstream),
    do: GenServer.cast(task, {:queue_frame, frame, direction})

  def stop_when_done(task), do: queue_frame(task, %EndFrame{})
  def cancel(task), do: queue_frame(task, %CancelFrame{})

  @doc "Blocks until the pipeline finishes. Returns `:ok` or `{:error, reason}`."
  def await(task, timeout \\ :infinity) do
    ref = Process.monitor(task)

    receive do
      {:DOWN, ^ref, :process, _pid, :normal} -> :ok
      {:DOWN, ^ref, :process, _pid, reason} -> {:error, reason}
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        {:error, :timeout}
    end
  end

  @doc "Convenience: start, run to completion, return `await/1` result."
  def run(opts) do
    {:ok, task} = start_link(opts)
    await(task)
  end

  # -- GenServer --

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    specs =
      [{Pipeline.Source, []}] ++
        Pipeline.normalize(Keyword.fetch!(opts, :processors)) ++ [{Pipeline.Sink, []}]

    pids =
      for {module, processor_opts} <- specs do
        bookend_opts = if module in [Pipeline.Source, Pipeline.Sink], do: [task: self()], else: []
        {:ok, pid} = Server.start_link(module, processor_opts ++ bookend_opts)
        pid
      end

    for {pid, index} <- Enum.with_index(pids) do
      Server.link(pid, next: Enum.at(pids, index + 1), prev: if(index > 0, do: Enum.at(pids, index - 1)))
    end

    [source | _rest] = pids
    start_frame = struct!(StartFrame, Keyword.get(opts, :params, []))
    Server.push(source, start_frame, :downstream)

    {:ok,
     %{
       pids: pids,
       source: source,
       subscriber: Keyword.get(opts, :subscriber),
       stopping?: false
     }}
  end

  @impl true
  def handle_cast({:queue_frame, frame, :downstream}, state) do
    Server.push(state.source, frame, :downstream)
    {:noreply, state}
  end

  @impl true
  def handle_info({:feline_pipeline, :downstream, %EndFrame{}}, state), do: shutdown(state)
  def handle_info({:feline_pipeline, :downstream, %CancelFrame{}}, state), do: shutdown(state)

  def handle_info({:feline_pipeline, direction, frame}, state) do
    case frame do
      %ErrorFrame{fatal: true} = frame ->
        Logger.error("#{inspect(__MODULE__)}: fatal error, cancelling pipeline: #{frame.error}")
        cancel(self())

      %ErrorFrame{} = frame ->
        Logger.warning("#{inspect(__MODULE__)}: pipeline error: #{frame.error}")

      _other ->
        :ok
    end

    if state.subscriber, do: send(state.subscriber, {:feline_pipeline, direction, frame})
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, pid, reason}, state) do
    if pid in state.pids and not state.stopping? do
      Logger.error("#{inspect(__MODULE__)}: processor #{inspect(pid)} crashed: #{inspect(reason)}")
      {:stop, reason, stop_processors(state)}
    else
      {:noreply, state}
    end
  end

  defp shutdown(state) do
    {:stop, :normal, stop_processors(state)}
  end

  defp stop_processors(state) do
    state = %{state | stopping?: true}

    for pid <- state.pids, Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end
    end

    state
  end
end
