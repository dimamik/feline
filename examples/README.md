# Feline demo: pipecat client, Elixir backend

The backend is Feline; the frontend is pipecat's own client stack
(`@pipecat-ai/client-js` + `@pipecat-ai/websocket-transport`) - unchanged,
exactly as it would talk to a Python pipecat server.

## 1. Start the bot

```sh
cd feline
cp .env.example .env
# edit .env and set:
# DEEPGRAM_API_KEY=...
# OPENAI_API_KEY=...
# ELEVENLABS_API_KEY=...
# optional: ELEVENLABS_VOICE_ID=...

mix run examples/voice_bot.exs
```

Listens on `ws://localhost:7860/ws`.

## 2. Start the client

```sh
cd feline/examples/client
npm install
npm run dev
```

Open http://localhost:5173, hit **Connect**, allow the microphone, and talk.
Interrupt the bot mid-sentence - it stops. Ask "what time is it" to see a
function call round-trip. You can also type instead of speaking.

## Notes

- The bot uses an energy-threshold VAD, so it works best in a quiet room
  (swap in a Silero analyzer later for noisy environments).
- Ports/keys are wired in `examples/voice_bot.exs`; the pipeline there is the
  whole story - transport, RTVI, VAD, turns, STT, context, LLM, TTS, reporter.
