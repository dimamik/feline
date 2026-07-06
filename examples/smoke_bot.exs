# Boots the demo pipeline against local fake AI services - used by the wire
# compatibility smoke test (examples/client/smoke.mjs). No API keys needed.
Code.require_file("test/support/fake_servers.ex")

alias Feline.FakeServers
alias Feline.FakeServers.{FakeCartesia, FakeDeepgram, FakeOpenAI, WSUpgrade}
alias Feline.Pipeline.Task, as: PipelineTask
alias Feline.Transports.WebSocket, as: Transport

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
      {Feline.Services.OpenAI.LLM, base_url: "http://localhost:#{openai_port}", api_key: "test"},
      Feline.Processors.SentenceAggregator,
      {Feline.Services.Cartesia.TTS, url: "ws://localhost:#{cartesia_port}/tts"},
      Feline.Processors.AssistantCollector,
      Feline.RTVI.Reporter,
      Transport.Output.spec(transport)
    ]
  )
end

{:ok, _server} = Transport.start_link(port: 7861, bot: bot)
IO.puts("SMOKE BOT READY on ws://localhost:7861/ws")
Process.sleep(:infinity)
