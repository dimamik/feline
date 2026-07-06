# A complete voice bot compatible with pipecat web clients.
#
# Run:  mix run examples/voice_bot.exs
# Then start the client in examples/client (see examples/README.md).
#
# Reads API keys from .env in the project root.
# Required keys: DEEPGRAM_API_KEY, OPENAI_API_KEY, ELEVENLABS_API_KEY
# Optional key: ELEVENLABS_VOICE_ID

alias Feline.Pipeline.Task, as: PipelineTask
alias Feline.Transports.WebSocket, as: Transport

defmodule Feline.Examples.Env do
  @moduledoc false

  def load(path) do
    if File.exists?(path) do
      path
      |> File.stream!([], :line)
      |> Enum.each(&load_line/1)
    end

    :ok
  end

  defp load_line(line) do
    line = String.trim(line)

    cond do
      line == "" or String.starts_with?(line, "#") ->
        :ok

      true ->
        case String.split(line, "=", parts: 2) do
          [key, value] ->
            key = String.trim(key)
            value = parse_value(value)

            if System.get_env(key) in [nil, ""] do
              System.put_env(key, value)
            end

          _other ->
            :ok
        end
    end
  end

  defp parse_value(raw_value) do
    value = String.trim(raw_value)

    cond do
      String.starts_with?(value, "\"") and String.ends_with?(value, "\"") ->
        value |> String.trim_leading("\"") |> String.trim_trailing("\"")

      String.starts_with?(value, "'") and String.ends_with?(value, "'") ->
        value |> String.trim_leading("'") |> String.trim_trailing("'")

      true ->
        value
        |> String.split(~r/\s+#/, parts: 2)
        |> hd()
        |> String.trim()
    end
  end
end

Feline.Examples.Env.load(Path.expand("../.env", __DIR__))

for variable <- ~w(DEEPGRAM_API_KEY OPENAI_API_KEY ELEVENLABS_API_KEY) do
  case System.get_env(variable) do
    nil -> raise "Missing #{variable}. Add it to feline/.env or export it in your shell."
    "" -> raise "Missing #{variable}. Add it to feline/.env or export it in your shell."
    _value -> :ok
  end
end

tools = [
  %{
    name: "get_current_time",
    description: "Get the current time",
    parameters: %{type: "object", properties: %{}},
    handler: fn _arguments ->
      %{"time" => DateTime.utc_now() |> DateTime.to_iso8601()}
    end
  }
]

bot = fn transport ->
  PipelineTask.start_link(
    processors: [
      Transport.Input.spec(transport),
      Feline.RTVI.Processor,
      Feline.Audio.EnergyVAD,
      Feline.Turns.UserTurnProcessor,
      Feline.Services.Deepgram.STT,
      {Feline.Processors.ContextAggregator,
       system_prompt: """
       You are Feline, a friendly voice assistant running on the BEAM.
       Your answers are spoken aloud, so keep them short and conversational -
       one or two sentences. No markdown, no lists.
       """,
       greeting: "Hey! I'm Feline, your Elixir voice bot. How can I help?",
       tools: tools},
      {Feline.Services.OpenAI.LLM, tools: tools},
      Feline.Processors.SentenceAggregator,
      Feline.Services.ElevenLabs.TTS,
      Feline.Processors.AssistantCollector,
      Feline.RTVI.Reporter,
      Transport.Output.spec(transport)
    ]
  )
end

{:ok, _server} = Transport.start_link(port: 7860, bot: bot)
IO.puts("Feline voice bot listening on ws://localhost:7860/ws")
Process.sleep(:infinity)
