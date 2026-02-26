defmodule Feline.Transports.WebSocket.Plug do
  @moduledoc """
  Plug that upgrades matching HTTP requests to WebSocket connections.
  """
  @behaviour Plug

  alias Feline.Transports.WebSocket.Handler

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    path = Keyword.fetch!(opts, :path)
    handler_state = Keyword.fetch!(opts, :handler_state)

    if conn.request_path == path do
      WebSockAdapter.upgrade(conn, Handler, handler_state, [])
    else
      Plug.Conn.send_resp(conn, 404, "Not Found")
    end
  end
end
