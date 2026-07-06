defmodule Feline.FakeServers do
  @moduledoc "Local fake OpenAI/Deepgram/Cartesia servers for service tests."

  def start_http(plug) do
    {:ok, server} = Bandit.start_link(plug: plug, port: 0, startup_log: false)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {server, port}
  end

  defmodule FakeOpenAI do
    @moduledoc """
    Streams a canned SSE completion. If the request carries tools and no
    "tool" message yet, replies with a tool_call split across deltas
    (including one delta missing the "function" key - the trap real OpenAI
    springs); otherwise streams text content.
    """

    use Plug.Builder

    plug :dispatch

    def dispatch(conn, _opts) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = Jason.decode!(body)

      conn =
        conn
        |> Plug.Conn.put_resp_content_type("text/event-stream")
        |> Plug.Conn.send_chunked(200)

      events =
        if request["tools"] != nil and
             not Enum.any?(request["messages"], &(&1["role"] == "tool")) do
          tool_call_events()
        else
          text_events(request)
        end

      Enum.reduce(events, conn, fn event, conn ->
        {:ok, conn} = Plug.Conn.chunk(conn, "data: #{event}\n\n")
        conn
      end)
    end

    defp text_events(request) do
      text =
        if Enum.any?(request["messages"], &(&1["role"] == "tool")),
          do: ["It is ", "21 degrees. ", "Enjoy!"],
          else: ["Hello ", "from the ", "fake LLM. ", "Bye!"]

      Enum.map(text, fn part ->
        Jason.encode!(%{choices: [%{delta: %{content: part}}]})
      end) ++ ["[DONE]"]
    end

    defp tool_call_events do
      [
        Jason.encode!(%{
          choices: [
            %{
              delta: %{
                tool_calls: [
                  %{index: 0, id: "call_1", function: %{name: "get_weather", arguments: ""}}
                ]
              }
            }
          ]
        }),
        # Delta with no "function" key at all.
        Jason.encode!(%{choices: [%{delta: %{tool_calls: [%{index: 0}]}}]}),
        Jason.encode!(%{
          choices: [
            %{delta: %{tool_calls: [%{index: 0, function: %{arguments: "{\"city\":"}}]}}
          ]
        }),
        Jason.encode!(%{
          choices: [
            %{delta: %{tool_calls: [%{index: 0, function: %{arguments: "\"Krakow\"}"}}]}}
          ]
        }),
        "[DONE]"
      ]
    end
  end

  defmodule WSUpgrade do
    @moduledoc "Plug that upgrades every request to the given WebSock handler."
    @behaviour Plug

    @impl true
    def init(handler), do: handler

    @impl true
    def call(conn, handler), do: WebSockAdapter.upgrade(conn, handler, [], [])
  end

  defmodule FakeDeepgram do
    @moduledoc "Replies to any binary audio with interim then final transcripts."
    @behaviour WebSock

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_in({audio, opcode: :binary}, state) do
      if audio == :binary.copy(<<0>>, byte_size(audio)) do
        {:ok, state}
      else
        interim = deepgram_result("hello", false)
        final = deepgram_result("hello world.", true)
        {:push, [{:text, interim}, {:text, final}], state}
      end
    end

    def handle_in(_frame, state), do: {:ok, state}

    @impl true
    def handle_info(_message, state), do: {:ok, state}

    @impl true
    def terminate(_reason, _state), do: :ok

    defp deepgram_result(transcript, final?) do
      Jason.encode!(%{
        channel: %{alternatives: [%{transcript: transcript}]},
        is_final: final?
      })
    end
  end

  defmodule FakeElevenLabs do
    @moduledoc "HTTP synthesis fake: any POST returns 20 bytes of PCM."
    use Plug.Builder

    plug :dispatch

    def dispatch(conn, _opts) do
      {:ok, _body, conn} = Plug.Conn.read_body(conn)

      conn
      |> Plug.Conn.put_resp_content_type("audio/pcm")
      |> Plug.Conn.send_resp(200, :binary.copy(<<3, 4>>, 10))
    end
  end

  defmodule FakeCartesia do
    @moduledoc "Replies to synthesis requests with one audio chunk then done."
    @behaviour WebSock

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_in({request_json, opcode: :text}, state) do
      request = Jason.decode!(request_json)

      if request["cancel"] do
        {:ok, state}
      else
        context_id = request["context_id"]
        audio = :binary.copy(<<1, 2>>, 10)

        chunk =
          Jason.encode!(%{type: "chunk", context_id: context_id, data: Base.encode64(audio)})

        done = Jason.encode!(%{type: "done", context_id: context_id})
        {:push, [{:text, chunk}, {:text, done}], state}
      end
    end

    def handle_in(_frame, state), do: {:ok, state}

    @impl true
    def handle_info(_message, state), do: {:ok, state}

    @impl true
    def terminate(_reason, _state), do: :ok
  end
end
