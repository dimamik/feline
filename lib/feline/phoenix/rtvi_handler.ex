defmodule Feline.Phoenix.RTVIHandler do
  @moduledoc """
  WebSock handler that starts a Feline pipeline per WebSocket connection.

  Expects `pipeline_builder` in init state — a `fn(ws_pid) -> Pipeline.t()`
  that returns a pipeline spec including RTVI processors and a
  `WebSocket.Output` with `ws_pid` set.

  Binary frames are routed as `InputAudioRawFrame`, text frames as
  `InputTransportMessageFrame` (parsed JSON).
  """
  @behaviour WebSock

  alias Feline.Frames.{InputAudioRawFrame, InputTransportMessageFrame}
  alias Feline.Frame
  alias Feline.Pipeline
  alias Feline.TransportParams

  @impl WebSock
  def init(%{pipeline_builder: builder} = state) do
    params = Map.get(state, :params, %TransportParams{})
    ws_pid = self()
    pipeline = builder.(ws_pid)

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn_link(fn -> Pipeline.Task.run(task) end)

    {:ok, %{task: task, params: params}}
  end

  @impl WebSock
  def handle_in({data, [opcode: :binary]}, state) do
    if state.params.audio_in_enabled do
      frame = %InputAudioRawFrame{
        id: Frame.new_id(),
        audio: data,
        sample_rate: state.params.audio_in_sample_rate
      }

      Pipeline.Task.queue_frame(state.task, frame)
    end

    {:ok, state}
  end

  def handle_in({data, [opcode: :text]}, state) do
    case Jason.decode(data) do
      {:ok, payload} ->
        frame = %InputTransportMessageFrame{id: Frame.new_id(), payload: payload}
        Pipeline.Task.queue_frame(state.task, frame)

      {:error, _} ->
        :ok
    end

    {:ok, state}
  end

  @impl WebSock
  def handle_info({:send_audio, audio}, state) do
    {:push, {:binary, audio}, state}
  end

  def handle_info({:send_message, payload}, state) do
    {:push, {:text, Jason.encode!(payload)}, state}
  end

  def handle_info(_msg, state) do
    {:ok, state}
  end

  @impl WebSock
  def terminate(_reason, state) do
    if state[:task] do
      Pipeline.Task.cancel(state.task)
    end

    :ok
  end
end
