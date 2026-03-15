defmodule Feline.Phoenix.RTVIPlug do
  @moduledoc """
  Plug that upgrades HTTP connections to WebSocket and delegates
  to `Feline.Phoenix.RTVIHandler`.

  ## Options

    * `:pipeline_builder` (required) — `fn(ws_pid) -> Pipeline.t()`
    * `:params` — `%Feline.TransportParams{}`, defaults to sensible values
    * `:path` — WebSocket path, defaults to `"/ws"`

  ## Usage in Phoenix Router

      forward "/feline", Feline.Phoenix.RTVIPlug,
        pipeline_builder: &MyApp.VoicePipeline.build/1
  """
  @behaviour Plug

  alias Feline.Phoenix.RTVIHandler
  alias Feline.TransportParams

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    path_segments =
      Keyword.get(opts, :path, "/ws")
      |> String.split("/", trim: true)

    if conn.path_info == path_segments do
      handler_state = %{
        pipeline_builder: Keyword.fetch!(opts, :pipeline_builder),
        params: Keyword.get(opts, :params, %TransportParams{})
      }

      WebSockAdapter.upgrade(conn, RTVIHandler, handler_state, [])
    else
      Plug.Conn.send_resp(conn, 404, "Not Found")
    end
  end
end
