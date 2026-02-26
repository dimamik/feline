defmodule Feline.Transports.WebSocket.Server do
  @moduledoc """
  GenServer managing the Bandit WebSocket server and routing frames
  between WebSocket clients and the active pipeline.
  """
  use GenServer

  alias Feline.TransportParams
  alias Feline.Transports.WebSocket.Plug, as: WSPlug

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def register_pipeline(server, pipeline_pid) do
    GenServer.call(server, {:register_pipeline, pipeline_pid})
  end

  def get_pipeline_pid(server) do
    GenServer.call(server, :get_pipeline_pid)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    port = Keyword.get(opts, :port, 8765)
    path = Keyword.get(opts, :path, "/ws")
    params = Keyword.get(opts, :params, %TransportParams{})

    handler_state = %{server: name, params: params}
    plug = {WSPlug, path: path, handler_state: handler_state}

    {:ok, bandit} = Bandit.start_link(plug: plug, port: port, scheme: :http)

    {:ok, %{bandit: bandit, port: port, path: path, params: params, pipeline_pid: nil}}
  end

  @impl true
  def handle_call({:register_pipeline, pid}, _from, state) do
    {:reply, :ok, %{state | pipeline_pid: pid}}
  end

  def handle_call(:get_pipeline_pid, _from, state) do
    {:reply, state.pipeline_pid, state}
  end
end
