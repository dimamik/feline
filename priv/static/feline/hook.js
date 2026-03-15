/**
 * FelineVoiceBot — Phoenix LiveView Hook
 *
 * Attaches to a DOM element with `phx-hook="FelineVoiceBot"` and
 * manages a pipecat-client-web PipecatClient lifecycle.
 *
 * Data attributes:
 *   data-ws-url — WebSocket URL for the Feline RTVI endpoint (e.g. "/feline/ws")
 *
 * LiveView events (server → client):
 *   feline:connect    — start the voice session
 *   feline:disconnect — stop the voice session
 *
 * LiveView events (client → server):
 *   feline:connected      — WebSocket connected
 *   feline:disconnected   — WebSocket disconnected
 *   feline:bot-ready      — bot pipeline is ready
 *   feline:user-transcript — user speech transcription {text, final}
 *   feline:bot-llm-text   — streamed LLM token {text}
 *   feline:bot-tts-text   — TTS word being spoken {text}
 *   feline:error          — error {message, fatal}
 *   feline:transport-state — transport state changed {state}
 */

import { FelineWebSocketTransport } from "./transport.js";

export const FelineVoiceBot = {
  mounted() {
    this._client = null;
    this._transport = null;
    this._connected = false;

    // Listen for server-pushed events
    this.handleEvent("feline:connect", () => this._start());
    this.handleEvent("feline:disconnect", () => this._stop());
  },

  destroyed() {
    this._stop();
  },

  async _start() {
    if (this._connected) return;

    const wsUrl = this.el.dataset.wsUrl || "/feline/ws";

    this._transport = new FelineWebSocketTransport({ wsUrl });

    // If PipecatClient is available (user installed @pipecat-ai/client-js),
    // use it. Otherwise, drive the transport directly.
    if (typeof globalThis.PipecatClient !== "undefined") {
      this._client = new globalThis.PipecatClient({
        transport: this._transport,
        enableMic: true,
        enableCam: false,
        callbacks: this._buildCallbacks(),
      });

      try {
        await this._client.connect();
      } catch (err) {
        this.pushEvent("feline:error", { message: err.message, fatal: false });
      }
    } else {
      // Standalone mode — drive transport directly without PipecatClient
      this._transport.initialize(
        { callbacks: this._buildCallbacks() },
        (msg) => this._handleMessage(msg)
      );

      try {
        await this._transport.initDevices();
        await this._transport.connect();
        this._transport.sendReadyMessage();
        this._connected = true;
      } catch (err) {
        this.pushEvent("feline:error", { message: err.message, fatal: false });
      }
    }
  },

  async _stop() {
    if (this._client) {
      try {
        await this._client.disconnect();
      } catch (_) {}
      this._client = null;
    } else if (this._transport) {
      try {
        await this._transport.disconnect();
      } catch (_) {}
    }
    this._transport = null;
    this._connected = false;
  },

  _buildCallbacks() {
    return {
      onConnected: () => {
        this._connected = true;
        this.pushEvent("feline:connected", {});
      },
      onDisconnected: () => {
        this._connected = false;
        this.pushEvent("feline:disconnected", {});
      },
      onBotReady: () => {
        this.pushEvent("feline:bot-ready", {});
      },
      onTransportStateChanged: (state) => {
        this.pushEvent("feline:transport-state", { state });
      },
      onError: (err) => {
        this.pushEvent("feline:error", {
          message: err?.message || String(err),
          fatal: false,
        });
      },
    };
  },

  _handleMessage(msg) {
    // Route RTVI messages to LiveView as events
    switch (msg.type) {
      case "bot-ready":
        this.pushEvent("feline:bot-ready", msg.data || {});
        break;
      case "user-transcription":
        this.pushEvent("feline:user-transcript", msg.data || {});
        break;
      case "bot-llm-text":
        this.pushEvent("feline:bot-llm-text", msg.data || {});
        break;
      case "bot-tts-text":
        this.pushEvent("feline:bot-tts-text", msg.data || {});
        break;
      case "error":
        this.pushEvent("feline:error", msg.data || {});
        break;
      default:
        // Forward any other RTVI message generically
        this.pushEvent("feline:message", { type: msg.type, data: msg.data });
        break;
    }
  },
};

export default FelineVoiceBot;
