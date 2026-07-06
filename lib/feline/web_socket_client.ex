defmodule Feline.WebSocketClient do
  @moduledoc """
  Minimal WebSocket client over Mint, owned by a parent process (typically a
  processor server). Decoded frames are delivered to the parent as
  `{:websocket_frame, client_pid, {:text, data} | {:binary, data}}`; the
  connection closing delivers `{:websocket_closed, client_pid, reason}`.

  Linked to the parent: a connection crash takes the processor (and pipeline)
  down, which is the let-it-crash behavior we want for MVP.
  """

  use GenServer

  def start_link(url, opts \\ []) do
    GenServer.start_link(__MODULE__, {url, opts, self()})
  end

  def send_frame(client, frame), do: GenServer.cast(client, {:send_frame, frame})

  def close(client), do: GenServer.stop(client, :normal)

  # -- GenServer --

  @impl true
  def init({url, opts, parent}) do
    uri = URI.parse(url)
    {scheme, ws_scheme} = if uri.scheme == "wss", do: {:https, :wss}, else: {:http, :ws}
    path = (uri.path || "/") <> if uri.query, do: "?" <> uri.query, else: ""

    with {:ok, conn} <- Mint.HTTP.connect(scheme, uri.host, uri.port, protocols: [:http1]),
         {:ok, conn, ref} <-
           Mint.WebSocket.upgrade(ws_scheme, conn, path, Keyword.get(opts, :headers, [])) do
      state = %{
        conn: conn,
        ref: ref,
        websocket: nil,
        parent: parent,
        status: nil,
        headers: nil
      }

      {:ok, state}
    else
      {:error, reason} -> {:stop, {:connect_failed, reason}}
      {:error, _conn, reason} -> {:stop, {:upgrade_failed, reason}}
    end
  end

  @impl true
  def handle_cast({:send_frame, frame}, %{websocket: nil} = state) do
    # Upgrade still in flight; retry after it completes.
    send(self(), {:retry_send, frame})
    {:noreply, state}
  end

  def handle_cast({:send_frame, frame}, state) do
    {:noreply, do_send(state, frame)}
  end

  @impl true
  def handle_info({:retry_send, frame}, %{websocket: nil} = state) do
    Process.send_after(self(), {:retry_send, frame}, 10)
    {:noreply, state}
  end

  def handle_info({:retry_send, frame}, state), do: {:noreply, do_send(state, frame)}

  def handle_info(message, state) do
    case Mint.WebSocket.stream(state.conn, message) do
      {:ok, conn, responses} ->
        {:noreply, Enum.reduce(responses, %{state | conn: conn}, &handle_response/2)}

      {:error, _conn, reason, _responses} ->
        send(state.parent, {:websocket_closed, self(), reason})
        {:stop, :normal, state}

      :unknown ->
        {:noreply, state}
    end
  end

  defp handle_response({:status, ref, status}, %{ref: ref} = state), do: %{state | status: status}

  defp handle_response({:headers, ref, headers}, %{ref: ref} = state) do
    case Mint.WebSocket.new(state.conn, ref, state.status, headers) do
      {:ok, conn, websocket} -> %{state | conn: conn, websocket: websocket, headers: headers}
      {:error, _conn, reason} -> exit({:websocket_failed, reason})
    end
  end

  defp handle_response({:data, ref, data}, %{ref: ref} = state) do
    {:ok, websocket, frames} = Mint.WebSocket.decode(state.websocket, data)
    state = %{state | websocket: websocket}
    Enum.reduce(frames, state, &handle_ws_frame/2)
  end

  # For HTTP/1.1 upgrades Mint emits {:done, ref} at the end of the 101
  # response; the websocket stays open. Only treat it as a close when the
  # upgrade never completed.
  defp handle_response({:done, ref}, %{ref: ref, websocket: nil} = state) do
    send(state.parent, {:websocket_closed, self(), :done})
    state
  end

  defp handle_response({:done, ref}, %{ref: ref} = state), do: state

  defp handle_response(_response, state), do: state

  defp handle_ws_frame({:text, _data} = frame, state) do
    send(state.parent, {:websocket_frame, self(), frame})
    state
  end

  defp handle_ws_frame({:binary, _data} = frame, state) do
    send(state.parent, {:websocket_frame, self(), frame})
    state
  end

  defp handle_ws_frame({:ping, data}, state), do: do_send(state, {:pong, data})

  defp handle_ws_frame({:close, _code, _reason}, state) do
    send(state.parent, {:websocket_closed, self(), :closed_by_server})
    state
  end

  defp handle_ws_frame(_frame, state), do: state

  defp do_send(state, frame) do
    {:ok, websocket, data} = Mint.WebSocket.encode(state.websocket, frame)
    {:ok, conn} = Mint.WebSocket.stream_request_body(state.conn, state.ref, data)
    %{state | conn: conn, websocket: websocket}
  end
end
