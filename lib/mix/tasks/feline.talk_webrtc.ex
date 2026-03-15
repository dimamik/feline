defmodule Mix.Tasks.Feline.TalkWebrtc do
  @moduledoc """
  Browser-based voice agent using Boombox WebRTC for audio I/O.

  Opens a local web server. Navigate to the URL in your browser,
  click Start, and speak into your microphone.

  Requires Deepgram STT, OpenAI LLM, and Deepgram TTS.

  ## Environment variables (or .env file)

      OPENAI_API_KEY=...
      DEEPGRAM_API_KEY=...

  ## Usage

      mix feline.talk_webrtc
      mix feline.talk_webrtc --port 8831
      mix feline.talk_webrtc --system "You are a pirate. Respond in pirate speak."
  """
  use Mix.Task

  alias Feline.Pipeline
  alias Feline.Context
  alias Feline.Frames.{InputAudioRawFrame, LLMContextFrame}
  alias Feline.Processors.ContextAggregatorPair
  alias Feline.Transports.Boombox.{Plug, AudioOutput}
  alias Membrane.WebRTC.Signaling

  @sample_rate 16_000
  @chunk_bytes 640

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")
    load_dotenv()

    {opts, _} = OptionParser.parse!(args, strict: [system: :string, port: :integer])

    system_prompt =
      Keyword.get(opts, :system, "You are a helpful voice assistant. Keep responses brief.")

    port = Keyword.get(opts, :port, 8831)

    openai_key = require_env!("OPENAI_API_KEY")
    deepgram_key = require_env!("DEEPGRAM_API_KEY")

    # Create signaling channels for WebRTC negotiation
    {:ok, input_sig_pid} = Signaling.start_link([])
    input_signaling = Signaling.new(input_sig_pid)

    {:ok, output_sig_pid} = Signaling.start_link([])
    output_signaling = Signaling.new(output_sig_pid)

    # Start HTTP server for HTML page + signaling WebSocket endpoints
    {:ok, _bandit} =
      Bandit.start_link(
        plug: {Plug, input_signaling: input_signaling, output_signaling: output_signaling},
        port: port,
        scheme: :http
      )

    Mix.shell().info("Feline WebRTC voice assistant running.")
    Mix.shell().info("Open http://localhost:#{port} in your browser and click Start.")
    Mix.shell().info("Waiting for browser to connect...\n")

    # Start Boombox servers via the message API to avoid the default 5s GenServer.call timeout.
    # Boombox.run blocks until WebRTC negotiation completes (browser must connect first).
    # Both must run in parallel since each waits for its own WebRTC connection.
    reader_task = Task.async(fn -> start_boombox(:reader, input_signaling) end)
    writer_task = Task.async(fn -> start_boombox(:writer, output_signaling) end)

    reader = Task.await(reader_task, :infinity)
    writer = Task.await(writer_task, :infinity)

    Mix.shell().info("Browser connected! Speak into your microphone.\n")

    # Build Feline pipeline
    context = Context.new([%{"role" => "system", "content" => system_prompt}])
    {:ok, pair} = ContextAggregatorPair.start(context)

    pipeline =
      Pipeline.new([
        {Feline.Processors.VADProcessor, start_secs: 0.2, stop_secs: 0.8},
        {Feline.Services.Deepgram.StreamingSTT, api_key: deepgram_key, sample_rate: @sample_rate},
        {Feline.Processors.ConsoleLogger.UserInput, []},
        {Feline.Processors.UserContextAggregator, context_agent: pair.agent},
        {Feline.Services.OpenAI.StreamingLLM, api_key: openai_key, model: "gpt-4.1-mini"},
        {Feline.Processors.AssistantContextAggregator, context_agent: pair.agent},
        {Feline.Processors.ConsoleLogger.BotOutput, []},
        {Feline.Processors.SentenceAggregator, []},
        {Feline.Services.Deepgram.StreamingTTS, api_key: deepgram_key, sample_rate: 24_000},
        {AudioOutput, writer_pid: writer, sample_rate: 24_000}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn_link(fn -> Pipeline.Task.run(task) end)
    Process.sleep(100)

    spawn_stdin_reader(task, pair.agent)

    reader_loop(reader, task)
  end

  defp start_boombox(:reader, signaling) do
    pid =
      start_boombox_server(
        input: {:webrtc, signaling},
        output:
          {:reader,
           audio: :binary,
           video: false,
           audio_format: :s16le,
           audio_rate: @sample_rate,
           audio_channels: 1}
      )

    %Boombox.Reader{server_reference: pid}
  end

  defp start_boombox(:writer, signaling) do
    start_boombox_server(:messages,
      input: {:message, audio: :binary, video: false},
      output: {:webrtc, signaling}
    )
  end

  # Starts a Boombox.Server and sends {:run, opts} via the message API
  # to avoid the default 5s GenServer.call timeout. Waits indefinitely
  # for WebRTC negotiation to complete (browser must connect first).
  defp start_boombox_server(communication_medium \\ :calls, boombox_opts) do
    {:ok, pid} =
      Boombox.Server.start(
        packet_serialization: false,
        stop_application: false,
        communication_medium: communication_medium
      )

    send(pid, {:call, self(), {:run, boombox_opts}})

    receive do
      {:response, _mode} -> pid
    end
  end

  defp reader_loop(reader, task) do
    case Boombox.read(reader) do
      {:ok, %Boombox.Packet{payload: audio}} ->
        send_chunks(audio, task)
        reader_loop(reader, task)

      :finished ->
        Pipeline.Task.stop_when_done(task)
        Mix.shell().info("WebRTC connection closed.")
    end
  end

  defp send_chunks(buffer, task) when byte_size(buffer) >= @chunk_bytes do
    <<chunk::binary-size(@chunk_bytes), rest::binary>> = buffer

    frame = %InputAudioRawFrame{
      id: make_ref(),
      audio: chunk,
      sample_rate: @sample_rate
    }

    Pipeline.Task.queue_frame(task, frame)
    send_chunks(rest, task)
  end

  defp send_chunks(_buffer, _task), do: :ok

  defp spawn_stdin_reader(task, context_agent) do
    spawn_link(fn -> stdin_loop(task, context_agent) end)
  end

  defp stdin_loop(task, context_agent) do
    case IO.gets("") do
      :eof ->
        :ok

      {:error, _} ->
        :ok

      line ->
        text = String.trim(line)

        if text != "" do
          IO.puts(IO.ANSI.cyan() <> "You (typed): " <> IO.ANSI.reset() <> text)

          ContextAggregatorPair.append_message(context_agent, %{
            "role" => "user",
            "content" => text
          })

          context = ContextAggregatorPair.get_context(context_agent)

          Pipeline.Task.queue_frame(task, %LLMContextFrame{
            id: make_ref(),
            context: context
          })
        end

        stdin_loop(task, context_agent)
    end
  end

  defp load_dotenv do
    if File.exists?(".env") do
      ".env"
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.reject(&(String.starts_with?(&1, "#") or &1 == ""))
      |> Enum.each(&parse_env_line/1)
    end
  end

  defp parse_env_line(line) do
    case String.split(line, "=", parts: 2) do
      [key, value] -> System.put_env(String.trim(key), String.trim(value))
      _ -> :ok
    end
  end

  defp require_env!(key) do
    case System.get_env(key) do
      nil -> Mix.raise("Missing #{key} — add it to .env or export it")
      "" -> Mix.raise("Empty #{key} — add it to .env or export it")
      val -> val
    end
  end
end
