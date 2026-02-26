defmodule Feline.Transports.WebSocket.Input do
  @moduledoc """
  Processor that receives WebSocket messages via `handle_info` and
  converts them to `InputAudioRawFrame` or `InputTransportMessageFrame`.
  """
  use Feline.Processor

  alias Feline.Frames.{InputAudioRawFrame, InputTransportMessageFrame}
  alias Feline.TransportParams

  @impl true
  def init(opts) do
    params = Keyword.get(opts, :params, %TransportParams{})
    {:ok, %{params: params}}
  end

  @impl true
  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  @impl true
  def handle_info({:ws_binary, data}, push_fn, state) do
    if state.params.audio_in_enabled do
      frame = %InputAudioRawFrame{
        id: make_ref(),
        audio: data,
        sample_rate: state.params.audio_in_sample_rate
      }

      push_fn.(frame, :downstream)
    end

    {:ok, state}
  end

  def handle_info({:ws_text, data}, push_fn, state) do
    case Jason.decode(data) do
      {:ok, payload} ->
        frame = %InputTransportMessageFrame{id: make_ref(), payload: payload}
        push_fn.(frame, :downstream)

      {:error, _} ->
        :ok
    end

    {:ok, state}
  end

  def handle_info(_msg, _push_fn, state), do: {:ok, state}
end
