defmodule Feline.Transports.Boombox.Plug do
  @moduledoc """
  Plug that serves the WebRTC demo HTML page and upgrades
  signaling WebSocket connections for input and output channels.
  """
  @behaviour Plug

  alias Feline.Transports.Boombox.SignalingHandler

  @impl Plug
  def init(opts), do: Map.new(opts)

  @impl Plug
  def call(conn, opts) do
    case conn.request_path do
      "/" ->
        html = File.read!(Path.join(:code.priv_dir(:feline), "static/boombox_demo.html"))

        conn
        |> Plug.Conn.put_resp_content_type("text/html")
        |> Plug.Conn.send_resp(200, html)

      "/ws/input" ->
        WebSockAdapter.upgrade(
          conn,
          SignalingHandler,
          %{signaling: opts.input_signaling},
          []
        )

      "/ws/output" ->
        WebSockAdapter.upgrade(
          conn,
          SignalingHandler,
          %{signaling: opts.output_signaling},
          []
        )

      _ ->
        Plug.Conn.send_resp(conn, 404, "Not Found")
    end
  end
end
