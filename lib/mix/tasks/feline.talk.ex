defmodule Mix.Tasks.Feline.Talk do
  @moduledoc """
  Live voice agent — speak into your microphone and hear the agent respond.

  Requires ffmpeg and ffplay installed. Uses Deepgram STT, OpenAI LLM,
  and ElevenLabs TTS.

  ## Environment variables (or .env file)

      OPENAI_API_KEY=...
      DEEPGRAM_API_KEY=...
      ELEVENLABS_API_KEY=...
      ELEVENLABS_VOICE_ID=...

  ## Usage

      mix feline.talk
      mix feline.talk --system "You are a pirate. Respond in pirate speak."
  """
  use Mix.Task

  alias Feline.Pipeline
  alias Feline.Context
  alias Feline.Processors.ContextAggregatorPair
  alias Feline.Frames.{InputAudioRawFrame, LLMContextFrame}

  @sample_rate 16_000
  @chunk_bytes 640

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")
    load_dotenv()

    {opts, _} = OptionParser.parse!(args, strict: [system: :string])

    system_prompt =
      Keyword.get(opts, :system, "You are a helpful voice assistant. Keep responses brief.")

    openai_key = require_env!("OPENAI_API_KEY")
    deepgram_key = require_env!("DEEPGRAM_API_KEY")
    elevenlabs_key = require_env!("ELEVENLABS_API_KEY")
    voice_id = require_env!("ELEVENLABS_VOICE_ID")

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
        {Feline.Services.ElevenLabs.StreamingTTS,
         api_key: elevenlabs_key, voice_id: voice_id, sample_rate: 24_000},
        {Feline.Processors.AudioPlayer, sample_rate: 24_000}
      ])

    {:ok, task} = Pipeline.Task.start_link(pipeline)
    spawn_link(fn -> Pipeline.Task.run(task) end)
    Process.sleep(100)

    Mix.shell().info(
      "Listening... speak into your microphone or type a message. Ctrl+C to exit.\n"
    )

    spawn_stdin_reader(task, pair.agent)

    ffmpeg_log = Path.join(System.tmp_dir!(), "feline_ffmpeg.log")

    # Use a wrapper shell command so we can redirect stderr to a log file
    # while keeping stdout as clean binary PCM
    ffmpeg_cmd =
      "#{System.find_executable("ffmpeg")} " <>
        "-loglevel error -f avfoundation -i :0 " <>
        "-ac 1 -ar #{@sample_rate} -f s16le pipe:1 " <>
        "2>#{ffmpeg_log}"

    ffmpeg =
      Port.open(
        {:spawn, ffmpeg_cmd},
        [:binary, :exit_status]
      )

    # Wait briefly for ffmpeg to start and confirm we get data
    receive do
      {^ffmpeg, {:data, first_data}} ->
        Mix.shell().info("Mic capture started.")
        buffer = send_chunks(first_data, task)
        capture_loop(ffmpeg, task, buffer)

      {^ffmpeg, {:exit_status, code}} ->
        stderr = File.read(ffmpeg_log) |> elem(1) |> String.trim()
        Mix.raise("ffmpeg exited with code #{code}: #{stderr}")
    after
      5_000 ->
        stderr =
          case File.read(ffmpeg_log) do
            {:ok, s} -> String.trim(s)
            _ -> ""
          end

        msg =
          if stderr != "",
            do: "ffmpeg error: #{stderr}",
            else:
              "ffmpeg produced no audio after 5s — check microphone permissions (System Settings → Privacy & Security → Microphone)"

        Port.close(ffmpeg)
        Mix.raise(msg)
    end
  end

  defp capture_loop(ffmpeg, task, buffer) do
    receive do
      {^ffmpeg, {:data, data}} ->
        buffer = buffer <> data
        buffer = send_chunks(buffer, task)
        capture_loop(ffmpeg, task, buffer)

      {^ffmpeg, {:exit_status, 0}} ->
        Pipeline.Task.stop_when_done(task)
        Mix.shell().info("Microphone closed.")

      {^ffmpeg, {:exit_status, code}} ->
        Pipeline.Task.stop_when_done(task)
        Mix.shell().info("ffmpeg exited with code #{code}.")
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

  defp send_chunks(buffer, _task), do: buffer

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
      |> Enum.each(fn line ->
        case String.split(line, "=", parts: 2) do
          [key, value] -> System.put_env(String.trim(key), String.trim(value))
          _ -> :ok
        end
      end)
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
