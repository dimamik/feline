defmodule Feline.Transports.WebSocket.Handler do
  @moduledoc """
  WebSock handler that receives binary audio and text messages from
  clients and queues them as frames into the pipeline.
  """
  @behaviour WebSock

  alias Feline.Frames.{InputAudioRawFrame, InputTransportMessageFrame, CancelFrame}
  alias Feline.Processor
  alias Feline.Transports.WebSocket.Server

  @impl WebSock
  def init(state) do
    {:ok, state}
  end

  @impl WebSock
  def handle_in({data, [opcode: :binary]}, state) do
    if state.params.audio_in_enabled do
      frame = %InputAudioRawFrame{
        id: make_ref(),
        audio: data,
        sample_rate: state.params.audio_in_sample_rate
      }

      queue_frame(state.server, frame, :downstream)
    end

    {:ok, state}
  end

  def handle_in({data, [opcode: :text]}, state) do
    case Jason.decode(data) do
      {:ok, payload} ->
        frame = %InputTransportMessageFrame{id: make_ref(), payload: payload}
        queue_frame(state.server, frame, :downstream)

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
    pipeline_pid = Server.get_pipeline_pid(state.server)

    if is_pid(pipeline_pid) and Process.alive?(pipeline_pid) do
      frame = %CancelFrame{id: Feline.Frame.new_id()}
      Processor.queue_frame(pipeline_pid, frame, :downstream)
    end

    :ok
  end

  defp queue_frame(server, frame, direction) do
    case Server.get_pipeline_pid(server) do
      pid when is_pid(pid) -> Processor.queue_frame(pid, frame, direction)
      nil -> :ok
    end
  end
end
