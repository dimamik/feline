defmodule Feline.TransportE2ETest do
  use ExUnit.Case, async: true

  alias Feline.FakeServers
  alias Feline.FakeServers.{FakeCartesia, FakeDeepgram, FakeOpenAI, WSUpgrade}
  alias Feline.Pipeline.Task, as: PipelineTask
  alias Feline.Serializers.Protobuf
  alias Feline.Transports.WebSocket, as: Transport
  alias Feline.WebSocketClient

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputTransportMessageUrgentFrame,
    TranscriptionFrame
  }

  setup do
    {_server, openai_port} = FakeServers.start_http(FakeOpenAI)
    {_server, deepgram_port} = FakeServers.start_http({WSUpgrade, FakeDeepgram})
    {_server, cartesia_port} = FakeServers.start_http({WSUpgrade, FakeCartesia})

    bot = fn transport ->
      PipelineTask.start_link(
        processors: [
          Transport.Input.spec(transport),
          Feline.RTVI.Processor,
          {Feline.Audio.EnergyVAD, start_secs: 0.1, stop_secs: 0.2},
          Feline.Turns.UserTurnProcessor,
          {Feline.Services.Deepgram.STT, url: "ws://localhost:#{deepgram_port}/v1/listen"},
          {Feline.Processors.ContextAggregator, system_prompt: "You are a test bot."},
          {Feline.Services.OpenAI.LLM,
           base_url: "http://localhost:#{openai_port}", api_key: "test"},
          Feline.Processors.SentenceAggregator,
          {Feline.Services.Cartesia.TTS, url: "ws://localhost:#{cartesia_port}/tts"},
          Feline.Processors.AssistantCollector,
          Feline.RTVI.Reporter,
          Transport.Output.spec(transport)
        ]
      )
    end

    {:ok, server} = Transport.start_link(port: 0, bot: bot)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {:ok, client} = WebSocketClient.start_link("ws://localhost:#{port}/ws")

    %{client: client}
  end

  defp send_frame(client, frame),
    do: WebSocketClient.send_frame(client, {:binary, Protobuf.serialize(frame)})

  # A client MessageFrame is oneof field 4 with a JSON payload - the same wire
  # shape our serializer produces for output message frames.
  defp send_rtvi(client, message),
    do: send_frame(client, %OutputTransportMessageUrgentFrame{message: message})

  defp await(matcher, timeout \\ 2_000) do
    receive do
      {:websocket_frame, _pid, {:binary, data}} ->
        frame = Protobuf.deserialize(data)

        case matcher.(frame) do
          true -> frame
          false -> await(matcher, timeout)
        end
    after
      timeout -> flunk("expected frame not received")
    end
  end

  defp await_rtvi(type, timeout \\ 2_000) do
    frame =
      await(
        fn
          %InputTransportMessageFrame{message: %{"type" => ^type}} -> true
          _other -> false
        end,
        timeout
      )

    frame.message
  end

  defp loud_audio do
    %InputAudioRawFrame{
      audio: :binary.copy(<<5_000::little-signed-16>>, 1_600),
      sample_rate: 16_000,
      num_channels: 1
    }
  end

  defp silent_audio do
    %InputAudioRawFrame{
      audio: :binary.copy(<<0::little-signed-16>>, 1_600),
      sample_rate: 16_000,
      num_channels: 1
    }
  end

  test "client-ready handshake returns bot-ready", %{client: client} do
    send_rtvi(client, %{
      "label" => "rtvi-ai",
      "type" => "client-ready",
      "id" => "msg-1",
      "data" => %{"version" => "2.0.0", "about" => %{"library" => "test"}}
    })

    message = await_rtvi("bot-ready")
    assert message["id"] == "msg-1"
    assert message["data"]["version"] == "2.0.0"
    assert message["data"]["about"]["library"] == "feline"
  end

  test "full voice turn: audio in -> transcription, LLM, TTS audio and events out",
       %{client: client} do
    send_rtvi(client, %{"label" => "rtvi-ai", "type" => "client-ready", "id" => "1"})
    await_rtvi("bot-ready")

    for _index <- 1..2, do: send_frame(client, loud_audio())
    await_rtvi("user-started-speaking")

    # Transcriptions arrive while still speaking (await discards earlier
    # frames, so match these before user-stopped-speaking).
    final_transcription =
      await(fn
        %InputTransportMessageFrame{
          message: %{"type" => "user-transcription", "data" => %{"final" => true}}
        } ->
          true

        _other ->
          false
      end)

    assert final_transcription.message["data"]["text"] == "hello world."

    for _index <- 1..3, do: send_frame(client, silent_audio())
    await_rtvi("user-stopped-speaking")

    await_rtvi("bot-llm-started")
    assert %{"data" => %{"text" => "Hello "}} = await_rtvi("bot-llm-text")
    await_rtvi("bot-tts-started")

    audio_frame =
      await(fn
        %InputAudioRawFrame{} -> true
        _other -> false
      end)

    assert byte_size(audio_frame.audio) == 20
    await_rtvi("bot-started-speaking")
  end

  test "barge-in mid-response sends interruption + bot-interrupted to the client",
       %{client: client} do
    send_rtvi(client, %{"label" => "rtvi-ai", "type" => "client-ready", "id" => "1"})
    await_rtvi("bot-ready")

    for _index <- 1..2, do: send_frame(client, loud_audio())
    for _index <- 1..3, do: send_frame(client, silent_audio())

    await(fn
      %InputAudioRawFrame{} -> true
      _other -> false
    end)

    for _index <- 1..2, do: send_frame(client, loud_audio())

    await(fn
      %InterruptionFrame{} -> true
      _other -> false
    end)

    await_rtvi("bot-interrupted")
  end

  test "send-text runs the LLM without audio input", %{client: client} do
    send_rtvi(client, %{"label" => "rtvi-ai", "type" => "client-ready", "id" => "1"})
    await_rtvi("bot-ready")

    send_rtvi(client, %{
      "label" => "rtvi-ai",
      "type" => "send-text",
      "id" => "2",
      "data" => %{"content" => "hi bot"}
    })

    await_rtvi("bot-llm-started")
    assert %{"data" => %{"text" => "Hello "}} = await_rtvi("bot-llm-text")
  end
end
