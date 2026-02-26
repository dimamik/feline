defmodule Feline.Transports.WebSocket.TransportTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.TransportParams

  alias Feline.Transports.WebSocket.{Output, Input}

  alias Feline.Frames.{
    OutputAudioRawFrame,
    TTSAudioRawFrame,
    OutputTransportMessageFrame,
    InputAudioRawFrame,
    InputTransportMessageFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InterruptionFrame,
    EndFrame
  }

  defmodule Collector do
    use Feline.Processor

    @impl true
    def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      send(state.test_pid, {:collected, frame, direction})
      {:ok, state}
    end
  end

  # Default params: sample_rate=24000, 10ms_chunks=4
  # chunk_bytes = div(24000, 100) * 1 * 2 * 4 = 1920
  @chunk_bytes 1920

  describe "Output processor" do
    test "buffers audio and sends chunks to ws_pid" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      audio = :binary.copy(<<0>>, @chunk_bytes * 2)

      Processor.queue_frame(
        output,
        %OutputAudioRawFrame{id: make_ref(), audio: audio, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:send_audio, chunk}, 200
      assert byte_size(chunk) == @chunk_bytes
    end

    test "emits BotStartedSpeakingFrame on first audio" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      audio = :binary.copy(<<0>>, 100)

      Processor.queue_frame(
        output,
        %OutputAudioRawFrame{id: make_ref(), audio: audio, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:collected, %BotStartedSpeakingFrame{}, :downstream}, 200
    end

    test "emits BotStoppedSpeakingFrame when buffer drains" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      audio = :binary.copy(<<0>>, @chunk_bytes)

      Processor.queue_frame(
        output,
        %OutputAudioRawFrame{id: make_ref(), audio: audio, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:collected, %BotStartedSpeakingFrame{}, :downstream}, 200
      assert_receive {:send_audio, _chunk}, 200
      assert_receive {:collected, %BotStoppedSpeakingFrame{}, :downstream}, 200
    end

    test "InterruptionFrame clears buffer and stops speaking" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      audio = :binary.copy(<<0>>, @chunk_bytes * 10)

      Processor.queue_frame(
        output,
        %OutputAudioRawFrame{id: make_ref(), audio: audio, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:collected, %BotStartedSpeakingFrame{}, :downstream}, 200

      Processor.queue_frame(
        output,
        %InterruptionFrame{id: make_ref()},
        :downstream
      )

      assert_receive {:collected, %BotStoppedSpeakingFrame{}, :downstream}, 200
      assert_receive {:collected, %InterruptionFrame{}, :downstream}, 200

      # No more audio chunks should arrive after interruption
      refute_receive {:send_audio, _}, 100
    end

    test "EndFrame drains remaining audio" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      partial = :binary.copy(<<1>>, 500)

      Processor.queue_frame(
        output,
        %OutputAudioRawFrame{id: make_ref(), audio: partial, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:collected, %BotStartedSpeakingFrame{}, :downstream}, 200

      Processor.queue_frame(
        output,
        %EndFrame{id: make_ref()},
        :downstream
      )

      assert_receive {:send_audio, chunk}, 200
      assert chunk == partial

      assert_receive {:collected, %BotStoppedSpeakingFrame{}, :downstream}, 200
      assert_receive {:collected, %EndFrame{}, :downstream}, 200
    end

    test "sends text messages via OutputTransportMessageFrame" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      Process.sleep(50)

      payload = %{"type" => "greeting", "text" => "hello"}

      Processor.queue_frame(
        output,
        %OutputTransportMessageFrame{id: make_ref(), payload: payload},
        :downstream
      )

      assert_receive {:send_message, ^payload}, 200
    end

    test "TTSAudioRawFrame is handled the same as OutputAudioRawFrame" do
      {:ok, output} =
        Processor.Server.start_link({Output, [ws_pid: self(), params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(output, collector)
      Process.sleep(50)

      audio = :binary.copy(<<0>>, @chunk_bytes)

      Processor.queue_frame(
        output,
        %TTSAudioRawFrame{id: make_ref(), audio: audio, sample_rate: 24_000},
        :downstream
      )

      assert_receive {:collected, %BotStartedSpeakingFrame{}, :downstream}, 200
      assert_receive {:send_audio, chunk}, 200
      assert byte_size(chunk) == @chunk_bytes
    end
  end

  describe "Input processor" do
    test "ws_binary creates InputAudioRawFrame" do
      {:ok, input} =
        Processor.Server.start_link({Input, [params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(input, collector)
      Process.sleep(50)

      audio_data = :binary.copy(<<42>>, 320)
      send(input, {:ws_binary, audio_data})

      assert_receive {:collected, %InputAudioRawFrame{audio: ^audio_data, sample_rate: 16_000},
                      :downstream},
                     200
    end

    test "ws_text creates InputTransportMessageFrame" do
      {:ok, input} =
        Processor.Server.start_link({Input, [params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(input, collector)
      Process.sleep(50)

      payload = %{"type" => "config", "value" => 42}
      json = Jason.encode!(payload)
      send(input, {:ws_text, json})

      assert_receive {:collected, %InputTransportMessageFrame{payload: ^payload}, :downstream},
                     200
    end

    test "respects audio_in_enabled: false" do
      {:ok, input} =
        Processor.Server.start_link({Input, [params: %TransportParams{audio_in_enabled: false}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(input, collector)
      Process.sleep(50)

      audio_data = :binary.copy(<<42>>, 320)
      send(input, {:ws_binary, audio_data})

      refute_receive {:collected, %InputAudioRawFrame{}, :downstream}, 100
    end

    test "ignores invalid JSON in ws_text" do
      {:ok, input} =
        Processor.Server.start_link({Input, [params: %TransportParams{}]})

      {:ok, collector} = Processor.Server.start_link({Collector, [test_pid: self()]})
      Processor.link(input, collector)
      Process.sleep(50)

      send(input, {:ws_text, "not valid json {{"})

      refute_receive {:collected, %InputTransportMessageFrame{}, :downstream}, 100
    end
  end
end
