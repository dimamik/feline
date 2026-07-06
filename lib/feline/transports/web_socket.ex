defmodule Feline.Transports.WebSocket do
  @moduledoc """
  WebSocket server transport speaking pipecat's protobuf wire format -
  compatible with `@pipecat-ai/client-js` + `@pipecat-ai/websocket-transport`.

  Start one server per bot:

      Feline.Transports.WebSocket.start_link(
        port: 7860,
        bot: fn transport ->
          Feline.Pipeline.Task.start_link(
            processors:
              [Feline.Transports.WebSocket.Input.spec(transport)] ++
                bot_processors() ++
                [Feline.Transports.WebSocket.Output.spec(transport)]
          )
        end
      )

  Each client connection spawns a fresh pipeline via the `:bot` function,
  which receives the connection handler pid (`transport`) and must return
  `{:ok, pipeline_task}`.
  """

  def start_link(opts) do
    port = Keyword.get(opts, :port, 7860)
    bot = Keyword.fetch!(opts, :bot)

    Bandit.start_link(
      plug: {Feline.Transports.WebSocket.UpgradePlug, bot},
      port: port
    )
  end

  defmodule UpgradePlug do
    @moduledoc false
    @behaviour Elixir.Plug

    @impl true
    def init(bot), do: bot

    @impl true
    def call(conn, bot) do
      WebSockAdapter.upgrade(conn, Feline.Transports.WebSocket.Handler, %{bot: bot}, [])
    end
  end

  defmodule Handler do
    @moduledoc false
    @behaviour WebSock

    alias Feline.Pipeline.Task, as: PipelineTask
    alias Feline.Serializers.Protobuf

    require Logger

    @impl true
    def init(%{bot: bot}) do
      {:ok, task} = bot.(self())
      monitor = Process.monitor(task)
      {:ok, %{task: task, monitor: monitor, input: nil, queued: []}}
    end

    @impl true
    def handle_in({data, opcode: :binary}, state) do
      case Protobuf.deserialize(data) do
        nil ->
          {:ok, state}

        frame ->
          if state.input do
            send(state.input, {:client_frame, frame})
            {:ok, state}
          else
            # The pipeline's input processor hasn't registered yet (StartFrame
            # still propagating); buffer so early client-ready isn't dropped.
            {:ok, %{state | queued: [frame | state.queued]}}
          end
      end
    end

    def handle_in(_frame, state), do: {:ok, state}

    @impl true
    def handle_info({:register_input, pid}, state) do
      for frame <- Enum.reverse(state.queued), do: send(pid, {:client_frame, frame})
      {:ok, %{state | input: pid, queued: []}}
    end

    def handle_info({:transport_send, data}, state), do: {:push, {:binary, data}, state}

    def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{monitor: monitor} = state) do
      {:stop, :normal, %{state | task: nil}}
    end

    def handle_info(_message, state), do: {:ok, state}

    @impl true
    def terminate(reason, %{task: task}) when is_pid(task) do
      Logger.info("Client disconnected (#{inspect(reason)}), cancelling pipeline")
      if Process.alive?(task), do: PipelineTask.cancel(task)
      :ok
    end

    def terminate(_reason, _state), do: :ok
  end

  defmodule Input do
    @moduledoc """
    Pipeline entry point: forwards frames deserialized by the connection
    handler into the pipeline.
    """

    use Feline.Processor

    def spec(transport), do: {__MODULE__, transport: transport}

    @impl true
    def handle_setup(_start_frame, ctx, %{transport: transport} = state) do
      send(transport, {:register_input, ctx.self})
      {:ok, state}
    end

    @impl true
    def handle_info({:client_frame, frame}, ctx, state) do
      ctx.push.(frame, :downstream)
      {:ok, state}
    end

    def handle_info(_message, _ctx, state), do: {:ok, state}
  end

  defmodule Output do
    @moduledoc """
    Pipeline exit point: serializes outbound frames to the client and derives
    bot-speaking state from the audio flow (started on first audio chunk,
    stopped when the accumulated playback clock runs out or an interruption
    cuts it short).
    """

    use Feline.Processor

    alias Feline.Serializers.Protobuf

    alias Feline.Frames.All.{
      BotStartedSpeakingFrame,
      BotStoppedSpeakingFrame,
      InterruptionFrame,
      OutputAudioRawFrame,
      OutputTransportMessageFrame,
      OutputTransportMessageUrgentFrame,
      TranscriptionFrame,
      TTSAudioRawFrame
    }

    def spec(transport), do: {__MODULE__, transport: transport}

    @impl true
    def init(opts) do
      {:ok, %{transport: Keyword.fetch!(opts, :transport), speaking_until: nil}}
    end

    @impl true
    def handle_frame(%module{} = frame, :downstream, ctx, state)
        when module in [TTSAudioRawFrame, OutputAudioRawFrame] do
      transmit(state, frame)
      {:ok, track_speaking(state, ctx, frame)}
    end

    def handle_frame(%InterruptionFrame{} = frame, direction, ctx, state) do
      # The client flushes its playback buffer when it receives this.
      transmit(state, frame)

      state =
        if state.speaking_until do
          broadcast_speaking(ctx, %BotStoppedSpeakingFrame{})
          %{state | speaking_until: nil}
        else
          state
        end

      {:push, frame, direction, state}
    end

    def handle_frame(%module{} = frame, :downstream, _ctx, state)
        when module in [
               OutputTransportMessageFrame,
               OutputTransportMessageUrgentFrame,
               TranscriptionFrame
             ] do
      transmit(state, frame)
      {:ok, state}
    end

    def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

    @impl true
    def handle_info(:speaking_check, ctx, state) do
      if state.speaking_until && now_ms() >= state.speaking_until do
        broadcast_speaking(ctx, %BotStoppedSpeakingFrame{})
        {:ok, %{state | speaking_until: nil}}
      else
        if state.speaking_until do
          delay_ms = state.speaking_until - now_ms() + 20
          schedule_speaking_check(ctx, delay_ms)
        end

        {:ok, state}
      end
    end

    def handle_info(_message, _ctx, state), do: {:ok, state}

    defp transmit(state, frame) do
      if data = Protobuf.serialize(frame) do
        send(state.transport, {:transport_send, data})
      end
    end

    defp track_speaking(state, ctx, frame) do
      chunk_ms = audio_chunk_ms(frame)

      case state.speaking_until do
        nil ->
          broadcast_speaking(ctx, %BotStartedSpeakingFrame{})
          schedule_speaking_check(ctx, chunk_ms + 200)
          %{state | speaking_until: now_ms() + chunk_ms + 200}

        speaking_until ->
          %{state | speaking_until: max(speaking_until, now_ms() + 200) + chunk_ms}
      end
    end

    defp audio_chunk_ms(frame) do
      div(byte_size(frame.audio) * 1_000, 2 * frame.sample_rate * frame.num_channels)
    end

    defp schedule_speaking_check(ctx, delay_ms) do
      Process.send_after(ctx.self, :speaking_check, max(delay_ms, 0))
    end

    defp broadcast_speaking(ctx, frame) do
      ctx.push.(frame, :upstream)
      ctx.push.(frame, :downstream)
    end

    defp now_ms, do: System.monotonic_time(:millisecond)
  end
end
