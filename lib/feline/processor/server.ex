defmodule Feline.Processor.Server do
  @moduledoc """
  GenServer hosting a `Feline.Processor` module.

  Reproduces pipecat's two-queue invariant without a priority queue: every
  handler is fast, so the mailbox drains in microseconds and system frames are
  acted on almost immediately. Anything slow runs in a monitored task; while
  one is running, data/control frames buffer in `pending` (order preserved).
  An `InterruptionFrame` kills the task and flushes `pending`, keeping
  uninterruptible frames. Flushable content therefore always lives either in
  `pending` or in the module's own state - never stuck in the mailbox.
  """

  use GenServer

  alias Feline.Frame

  alias Feline.Frames.All.{
    CancelFrame,
    InterruptionFrame,
    StartFrame
  }

  defstruct [:module, :mod_state, :name, :next, :prev, :task, pending: :queue.new()]

  # -- API --

  def start_link(module, opts \\ [], name \\ nil) do
    GenServer.start_link(__MODULE__, {module, opts, name || module})
  end

  def link(pid, next: next, prev: prev), do: GenServer.call(pid, {:link, next, prev})

  def push(nil, _frame, _direction), do: :ok
  def push(pid, frame, direction), do: send(pid, {:feline_frame, frame, direction}) && :ok

  # -- GenServer --

  @impl true
  def init({module, opts, name}) do
    Process.flag(:trap_exit, true)
    {:ok, mod_state} = module.init(opts)
    {:ok, %__MODULE__{module: module, mod_state: mod_state, name: name}}
  end

  @impl true
  def handle_call({:link, next, prev}, _from, state) do
    {:reply, :ok, %{state | next: next, prev: prev}}
  end

  @impl true
  def handle_info({:feline_frame, frame, direction}, state) do
    if Frame.system?(frame) do
      handle_system(frame, direction, state)
    else
      if state.task || !:queue.is_empty(state.pending) do
        {:noreply, %{state | pending: :queue.in({frame, direction}, state.pending)}}
      else
        dispatch(frame, direction, state)
      end
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %{ref: ref}} = state) do
    case reason do
      normal when normal in [:normal, :killed] -> drain(%{state | task: nil})
      other -> {:stop, other, state}
    end
  end

  def handle_info(message, state) do
    state.module.handle_info(message, ctx(state), state.mod_state)
    |> interpret(state)
  end

  @impl true
  def terminate(_reason, state) do
    if state.task, do: kill_task(state.task)
    state.module.handle_cleanup(state.mod_state)
  end

  # -- System frames --

  defp handle_system(%StartFrame{} = frame, direction, state) do
    {:ok, mod_state} = state.module.handle_setup(frame, ctx(state), state.mod_state)
    do_push(frame, direction, state)
    {:noreply, %{state | mod_state: mod_state}}
  end

  defp handle_system(%InterruptionFrame{} = frame, direction, state) do
    state = interrupt(state)

    case dispatch(frame, direction, state) do
      {:noreply, %{task: nil} = state} -> drain(state)
      other -> other
    end
  end

  defp handle_system(%CancelFrame{} = frame, direction, state) do
    if state.task, do: kill_task(state.task)
    state = %{state | task: nil, pending: :queue.new()}
    dispatch(frame, direction, state)
  end

  defp handle_system(frame, direction, state), do: dispatch(frame, direction, state)

  defp interrupt(%{task: task} = state) do
    task =
      if task && !(task.frame && Frame.uninterruptible?(task.frame)) do
        kill_task(task)
        nil
      else
        task
      end

    pending =
      state.pending
      |> :queue.to_list()
      |> Enum.filter(fn {frame, _direction} -> Frame.uninterruptible?(frame) end)
      |> :queue.from_list()

    %{state | task: task, pending: pending}
  end

  defp kill_task(%{pid: pid, ref: ref}) do
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    after
      1000 -> :ok
    end
  end

  # -- Dispatch & drain --

  defp dispatch(frame, direction, state) do
    state.module.handle_frame(frame, direction, ctx(state), state.mod_state)
    |> interpret(state, frame)
  end

  defp drain(state) do
    case :queue.out(state.pending) do
      {{:value, {frame, direction}}, pending} ->
        case dispatch(frame, direction, %{state | pending: pending}) do
          {:noreply, %{task: nil} = state} -> drain(state)
          other -> other
        end

      {:empty, _pending} ->
        {:noreply, state}
    end
  end

  defp interpret(result, state, frame \\ nil)

  defp interpret({:ok, mod_state}, state, _frame),
    do: {:noreply, %{state | mod_state: mod_state}}

  defp interpret({:push, frame, direction, mod_state}, state, _frame) do
    do_push(frame, direction, state)
    {:noreply, %{state | mod_state: mod_state}}
  end

  defp interpret({:push_many, pushes, mod_state}, state, _frame) do
    for {frame, direction} <- pushes, do: do_push(frame, direction, state)
    {:noreply, %{state | mod_state: mod_state}}
  end

  defp interpret({:async, fun, mod_state}, %{task: nil} = state, frame) do
    context = ctx(state)
    {pid, ref} = spawn_monitor(fn -> fun.(context) end)
    {:noreply, %{state | mod_state: mod_state, task: %{pid: pid, ref: ref, frame: frame}}}
  end

  defp do_push(frame, :downstream, state), do: push(state.next, frame, :downstream)
  defp do_push(frame, :upstream, state), do: push(state.prev, frame, :upstream)

  defp ctx(state) do
    next = state.next
    prev = state.prev

    push = fn
      frame, :downstream -> push(next, frame, :downstream)
      frame, :upstream -> push(prev, frame, :upstream)
    end

    %{push: push, self: self(), name: state.name}
  end
end
