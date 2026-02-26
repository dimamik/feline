defmodule Feline.Pipeline.Task do
  @moduledoc """
  Orchestrates pipeline execution. Starts processors under a DynamicSupervisor,
  links them in order, sends StartFrame, and manages the lifecycle.
  """
  use GenServer

  alias Feline.Frames.{StartFrame, EndFrame, CancelFrame}
  alias Feline.Processor

  defstruct [
    :pipeline,
    :opts,
    :supervisor,
    :source,
    :sink,
    :caller,
    all_pids: [],
    running: false
  ]

  def start_link(pipeline, opts \\ []) do
    GenServer.start_link(__MODULE__, {pipeline, opts})
  end

  def run(task) do
    GenServer.call(task, :run, :infinity)
  end

  def queue_frame(task, frame, direction \\ :downstream) do
    GenServer.cast(task, {:queue_frame, frame, direction})
  end

  def queue_frames(task, frames) do
    for frame <- frames, do: queue_frame(task, frame)
    :ok
  end

  def stop_when_done(task) do
    queue_frame(task, %EndFrame{id: Feline.Frame.new_id()})
  end

  def cancel(task) do
    queue_frame(task, %CancelFrame{id: Feline.Frame.new_id()})
  end

  @impl true
  def init({pipeline, opts}) do
    {:ok, %__MODULE__{pipeline: pipeline, opts: opts}}
  end

  @impl true
  def handle_call(:run, from, state) do
    {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)

    # Start source and sink
    {:ok, source} =
      DynamicSupervisor.start_child(
        sup,
        {Feline.Processor.Server, {Feline.Pipeline.Source, [task_pid: self()]}}
      )

    {:ok, sink} =
      DynamicSupervisor.start_child(
        sup,
        {Feline.Processor.Server, {Feline.Pipeline.Sink, [task_pid: self()]}}
      )

    # Start user processors
    user_pids =
      Enum.map(state.pipeline.processor_specs, fn {mod, opts} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            sup,
            {Feline.Processor.Server, {mod, opts}}
          )

        pid
      end)

    all_pids = [source | user_pids] ++ [sink]

    # Link processors in order
    all_pids
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.each(fn [a, b] -> Processor.link(a, b) end)

    # Setup all processors
    setup = %{observer: state.opts[:observer]}
    for pid <- all_pids, do: Processor.setup(pid, setup)

    # Send StartFrame
    start_frame = %StartFrame{
      id: Feline.Frame.new_id(),
      audio_in_sample_rate: state.opts[:audio_in_sample_rate] || 16_000,
      audio_out_sample_rate: state.opts[:audio_out_sample_rate] || 24_000,
      enable_metrics: state.opts[:enable_metrics] || false,
      enable_usage_metrics: state.opts[:enable_usage_metrics] || false
    }

    Processor.queue_frame(source, start_frame, :downstream)

    # Monitor all processors
    for pid <- all_pids, do: Process.monitor(pid)

    # Start heartbeat if configured
    if heartbeat_ms = heartbeat_interval(state.opts) do
      Process.send_after(self(), :heartbeat, heartbeat_ms)
    end

    {:noreply,
     %{
       state
       | supervisor: sup,
         source: source,
         sink: sink,
         all_pids: all_pids,
         running: true,
         caller: from
     }}
  end

  @impl true
  def handle_cast({:queue_frame, frame, direction}, state) do
    if state.running do
      target = if direction == :downstream, do: state.source, else: state.sink
      Processor.queue_frame(target, frame, direction)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:downstream_frame, %EndFrame{}}, state) do
    finish(state, :ok)
  end

  def handle_info({:downstream_frame, %CancelFrame{}}, state) do
    finish(state, :cancelled)
  end

  def handle_info({:downstream_frame, _frame}, state) do
    {:noreply, state}
  end

  def handle_info({:upstream_frame, _frame}, state) do
    {:noreply, state}
  end

  def handle_info(:heartbeat, state) do
    if state.running do
      frame = %Feline.Frames.HeartbeatFrame{id: Feline.Frame.new_id()}
      Processor.queue_frame(state.source, frame, :downstream)

      if heartbeat_ms = heartbeat_interval(state.opts) do
        Process.send_after(self(), :heartbeat, heartbeat_ms)
      end
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, {:shutdown, _}}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    if state.running do
      finish(state, {:error, {:processor_crashed, pid, reason}})
    else
      {:noreply, state}
    end
  end

  defp finish(state, result) do
    if state.caller, do: GenServer.reply(state.caller, result)

    DynamicSupervisor.stop(state.supervisor, :normal)

    {:stop, :normal, %{state | running: false, caller: nil}}
  end

  defp heartbeat_interval(opts) do
    case opts[:heartbeat_secs] do
      nil -> nil
      secs -> trunc(secs * 1_000)
    end
  end
end
