defmodule Feline.Transports.WebSocket do
  @behaviour Feline.Transport

  alias Feline.Transports.WebSocket.{Input, Output}

  @impl true
  def input_spec(opts), do: {Input, opts}

  @impl true
  def output_spec(opts), do: {Output, opts}
end
