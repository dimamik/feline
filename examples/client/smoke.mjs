// Wire-compatibility smoke test: drives a running Feline server with the REAL
// pipecat client serializer (@pipecat-ai/websocket-transport), no browser.
import { ProtobufFrameSerializer } from "@pipecat-ai/websocket-transport";

const serializer = new ProtobufFrameSerializer();
const ws = new WebSocket("ws://localhost:7861/ws");

const received = [];
let audioSamples = 0;
const expect = (label, test) =>
  console.log(test ? `PASS ${label}` : `FAIL ${label}`);

ws.onopen = () => {
  ws.send(
    serializer.serializeMessage({
      label: "rtvi-ai",
      type: "client-ready",
      id: "smoke-1",
      data: { version: "2.0.0", about: { library: "smoke-test" } },
    })
  );

  // ~200ms loud audio (turn start), then ~300ms silence (turn stop)
  const loud = new Int16Array(1600).fill(5000);
  const quiet = new Int16Array(1600).fill(0);
  for (const chunk of [loud, loud, quiet, quiet, quiet]) {
    ws.send(serializer.serializeAudio(chunk.buffer, 16000, 1));
  }
};

ws.onmessage = async (event) => {
  let frame;
  try {
    frame = await serializer.deserialize(event.data);
  } catch {
    return; // same as the real transport: unknown frame kinds are dropped
  }
  if (!frame) return;
  if (frame.type === "message") received.push(frame.message.type);
  if (frame.type === "audio") audioSamples += frame.audio.length;
};

setTimeout(() => {
  ws.close();
  expect("bot-ready", received.includes("bot-ready"));
  expect("user-started-speaking", received.includes("user-started-speaking"));
  expect("user-transcription", received.includes("user-transcription"));
  expect("bot-llm-text", received.includes("bot-llm-text"));
  expect("bot-tts-started", received.includes("bot-tts-started"));
  expect("tts audio received", audioSamples > 0);
  process.exit(received.includes("bot-ready") && audioSamples > 0 ? 0 : 1);
}, 3000);
