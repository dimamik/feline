defmodule Feline.Transports.Boombox.TextHandler do
  @moduledoc """
  WebSock handler that sends transcript and bot response text to the browser.
  """
  @behaviour WebSock

  @impl true
  def init(opts) do
    handler_pid = self()
    Agent.update(opts.registry, fn _ -> handler_pid end)
    {:ok, %{}}
  end

  @impl true
  def handle_in(_msg, state), do: {:ok, state}

  @impl true
  def handle_info({:text_message, payload}, state) do
    {:push, {:text, Jason.encode!(payload)}, state}
  end

  def handle_info(_msg, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok
end
