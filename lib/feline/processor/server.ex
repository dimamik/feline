defmodule Feline.Processor.Server do
  @moduledoc """
  GenServer that wraps a `Feline.Processor` callback module.

  Handles frame routing between processors, system frame prioritization
  via selective receive, pause/cancel state, and telemetry emission.
  Delegates frame processing to the wrapped module's callbacks.
  """
  use GenServer

  alias Feline.Frame
  alias Feline.Frames.{StartFrame, CancelFrame, InterruptionFrame, ErrorFrame}

  defstruct [
    :mod,
    :user_state,
    :next,
    :prev,
    :observer,
    started: false,
    paused: false,
    cancelling: false,
    has_handle_info_3: false,
    buffered: :queue.new()
  ]

  def start_link({mod, opts}) do
    GenServer.start_link(__MODULE__, {mod, opts})
  end

  @impl true
  def init({mod, opts}) do
    {:ok, user_state} = mod.init(opts)

    {:ok,
     %__MODULE__{
       mod: mod,
       user_state: user_state,
       has_handle_info_3: function_exported?(mod, :handle_info, 3)
     }}
  end

  @impl true
  def handle_call({:link_next, next_pid}, _from, state) do
    {:reply, :ok, %{state | next: next_pid}}
  end

  def handle_call({:link_prev, prev_pid}, _from, state) do
    {:reply, :ok, %{state | prev: prev_pid}}
  end

  def handle_call({:setup, setup}, _from, state) do
    state = %{state | observer: setup[:observer]}

    if function_exported?(state.mod, :handle_setup, 2) do
      {:ok, user_state} = state.mod.handle_setup(setup, state.user_state)
      {:reply, :ok, %{state | user_state: user_state}}
    else
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:system_frame, frame, direction}, state) do
    state = process_system_frame(frame, direction, state)
    {:noreply, state}
  end

  def handle_info({:frame, _frame, _direction}, %{cancelling: true} = state) do
    {:noreply, state}
  end

  def handle_info({:frame, frame, direction}, %{paused: true} = state) do
    if Frame.uninterruptible?(frame) do
      state = drain_system_frames(state)
      state = dispatch_to_user(frame, direction, state)
      {:noreply, state}
    else
      {:noreply, %{state | buffered: :queue.in({frame, direction}, state.buffered)}}
    end
  end

  def handle_info({:frame, frame, direction}, state) do
    state = drain_system_frames(state)

    state =
      if state.cancelling do
        state
      else
        dispatch_to_user(frame, direction, state)
      end

    {:noreply, state}
  end

  # Forward unknown messages to callback module's handle_info/3 if defined
  def handle_info(msg, state) do
    if state.has_handle_info_3 do
      push_fn = build_push_fn(state)
      {:ok, user_state} = state.mod.handle_info(msg, push_fn, state.user_state)
      {:noreply, %{state | user_state: user_state}}
    else
      {:noreply, state}
    end
  end

  # -- System frame handling --

  defp process_system_frame(%StartFrame{} = frame, direction, state) do
    state = %{state | started: true}
    dispatch_to_user(frame, direction, state)
  end

  defp process_system_frame(%CancelFrame{} = frame, direction, state) do
    state = %{state | cancelling: true, buffered: :queue.new()}
    dispatch_to_user(frame, direction, state)
  end

  defp process_system_frame(%InterruptionFrame{} = frame, direction, state) do
    state = %{state | buffered: :queue.new()}
    dispatch_to_user(frame, direction, state)
  end

  defp process_system_frame(frame, direction, state) do
    dispatch_to_user(frame, direction, state)
  end

  # -- User module dispatch --

  defp dispatch_to_user(frame, direction, state) do
    start_time = System.monotonic_time()

    result =
      try do
        push_fn = build_push_fn(state)
        state.mod.handle_frame(frame, direction, push_fn, state.user_state)
      rescue
        e ->
          error_frame = %ErrorFrame{
            id: Feline.Frame.new_id(),
            message: Exception.message(e),
            exception: e,
            processor: state.mod,
            fatal: false
          }

          push_frame(error_frame, :upstream, state)
          {:ok, state.user_state}
      end

    duration = System.monotonic_time() - start_time

    metadata = %{processor: state.mod, frame: frame.__struct__, direction: direction}
    :telemetry.execute([:feline, :processor, :frame_processed], %{duration: duration}, metadata)
    notify_observer(state.observer, :on_process_frame, metadata)

    case result do
      {:ok, user_state} ->
        %{state | user_state: user_state}

      {:push, out_frame, out_dir, user_state} ->
        state = %{state | user_state: user_state}
        push_frame(out_frame, out_dir, state)
        state

      {:push_many, frames_and_dirs, user_state} ->
        state = %{state | user_state: user_state}
        for {f, d} <- frames_and_dirs, do: push_frame(f, d, state)
        state
    end
  end

  # -- Push function for streaming callbacks --

  defp build_push_fn(state) do
    next = state.next
    prev = state.prev
    mod = state.mod
    observer = state.observer

    fn frame, direction ->
      case direction do
        :downstream when is_pid(next) ->
          Feline.Processor.queue_frame(next, frame, :downstream)
          emit_push_telemetry(mod, observer, frame, :downstream)

        :upstream when is_pid(prev) ->
          Feline.Processor.queue_frame(prev, frame, :upstream)
          emit_push_telemetry(mod, observer, frame, :upstream)

        _ ->
          :ok
      end
    end
  end

  defp emit_push_telemetry(mod, observer, frame, direction) do
    metadata = %{processor: mod, frame: frame.__struct__, direction: direction}
    :telemetry.execute([:feline, :processor, :frame_pushed], %{}, metadata)
    notify_observer(observer, :on_push_frame, metadata)
  end

  # -- Frame routing --

  defp push_frame(frame, :downstream, %{next: next, mod: mod, observer: observer})
       when is_pid(next) do
    Feline.Processor.queue_frame(next, frame, :downstream)
    metadata = %{processor: mod, frame: frame.__struct__, direction: :downstream}
    :telemetry.execute([:feline, :processor, :frame_pushed], %{}, metadata)
    notify_observer(observer, :on_push_frame, metadata)
  end

  defp push_frame(frame, :upstream, %{prev: prev, mod: mod, observer: observer})
       when is_pid(prev) do
    Feline.Processor.queue_frame(prev, frame, :upstream)
    metadata = %{processor: mod, frame: frame.__struct__, direction: :upstream}
    :telemetry.execute([:feline, :processor, :frame_pushed], %{}, metadata)
    notify_observer(observer, :on_push_frame, metadata)
  end

  defp push_frame(_frame, _direction, _state), do: :ok

  # -- Observer notification --

  defp notify_observer(nil, _callback, _data), do: :ok

  defp notify_observer(observer, callback, data) do
    if function_exported?(observer, callback, 1), do: apply(observer, callback, [data])
    :ok
  end

  @impl true
  def terminate(_reason, state) do
    if function_exported?(state.mod, :handle_cleanup, 1) do
      state.mod.handle_cleanup(state.user_state)
    end

    :ok
  end

  # -- Priority drain --

  defp drain_system_frames(state) do
    receive do
      {:system_frame, frame, direction} ->
        state = process_system_frame(frame, direction, state)
        drain_system_frames(state)
    after
      0 -> state
    end
  end
end
