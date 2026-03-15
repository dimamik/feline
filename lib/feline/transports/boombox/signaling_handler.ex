defmodule Feline.Transports.Boombox.SignalingHandler do
  @moduledoc """
  WebSock handler that bridges a browser WebSocket connection to a
  `Membrane.WebRTC.Signaling` process for WebRTC negotiation.
  """
  @behaviour WebSock

  alias Membrane.WebRTC.Signaling

  @impl true
  def init(opts) do
    Signaling.register_peer(opts.signaling, message_format: :json_data)
    Process.send_after(self(), :keep_alive, 30_000)
    {:ok, %{signaling: opts.signaling}}
  end

  @impl true
  def handle_in({message, [opcode: :text]}, state) do
    Signaling.signal(state.signaling, Jason.decode!(message))
    {:ok, state}
  end

  def handle_in(_other, state), do: {:ok, state}

  @impl true
  def handle_info({:membrane_webrtc_signaling, _pid, message, _metadata}, state) do
    {:push, {:text, Jason.encode!(message)}, state}
  end

  def handle_info(:keep_alive, state) do
    Process.send_after(self(), :keep_alive, 30_000)
    {:push, {:text, Jason.encode!(%{type: "keep_alive", data: ""})}, state}
  end

  def handle_info(_msg, state), do: {:ok, state}
end
