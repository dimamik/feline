import { PipecatClient } from "@pipecat-ai/client-js";
import { WebSocketTransport } from "@pipecat-ai/websocket-transport";

const WS_URL =
  new URLSearchParams(location.search).get("ws") ?? "ws://localhost:7860/ws";

const toggleButton = document.getElementById("toggle");
const status = document.getElementById("status");
const transcript = document.getElementById("transcript");
const userSpeaking = document.getElementById("user-speaking");
const botSpeaking = document.getElementById("bot-speaking");
const textForm = document.getElementById("text-form");
const textInput = document.getElementById("text-input");

let botLine = null;

function addLine(kind, text) {
  const line = document.createElement("div");
  line.className = `line ${kind}`;
  line.textContent = text;
  transcript.appendChild(line);
  transcript.scrollTop = transcript.scrollHeight;
  return line;
}

const client = new PipecatClient({
  transport: new WebSocketTransport(),
  enableMic: true,
  callbacks: {
    onTransportStateChanged: (state) => {
      status.textContent = state;
      toggleButton.textContent = state === "disconnected" ? "Connect" : "Disconnect";
    },
    onBotReady: (data) => {
      addLine("event", `bot ready (server: ${data.about?.library} ${data.about?.library_version})`);
    },
    onUserStartedSpeaking: () => userSpeaking.classList.add("active"),
    onUserStoppedSpeaking: () => userSpeaking.classList.remove("active"),
    onBotStartedSpeaking: () => botSpeaking.classList.add("active"),
    onBotStoppedSpeaking: () => botSpeaking.classList.remove("active"),
    onUserTranscript: (data) => {
      if (data.final) addLine("user", `you: ${data.text}`);
    },
    onBotTtsText: (data) => {
      if (!botLine) botLine = addLine("bot", "bot: ");
      botLine.textContent += `${data.text} `;
    },
    onBotLlmStopped: () => {
      botLine = null;
    },
    onError: (message) => addLine("event", `error: ${JSON.stringify(message.data)}`),
  },
});

toggleButton.addEventListener("click", async () => {
  if (client.state === "disconnected") {
    await client.connect({ wsUrl: WS_URL });
  } else {
    await client.disconnect();
    userSpeaking.classList.remove("active");
    botSpeaking.classList.remove("active");
  }
});

textForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const content = textInput.value.trim();
  if (!content || client.state !== "ready") return;
  addLine("user", `you (typed): ${content}`);
  textInput.value = "";
  await client.sendText(content);
});
