/**
 * FelineWebSocketTransport — Custom Transport for @pipecat-ai/client-js
 *
 * Sends/receives raw PCM audio over a plain WebSocket to a Feline
 * (Elixir) backend. RTVI JSON messages go as text frames, audio as
 * binary frames.
 *
 * Audio capture: getUserMedia (48kHz) → AudioWorklet → downsample to 16kHz → Int16 PCM → WS binary
 * Audio playback: WS binary (24kHz Int16 PCM) → AudioWorklet → upsample to 48kHz → speakers
 */

// ---- AudioWorklet processor source (inlined, loaded via Blob URL) ----

const CAPTURE_PROCESSOR_SRC = `
class CaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    this.targetRate = (options.processorOptions && options.processorOptions.targetSampleRate) || 16000;
    this.nativeRate = sampleRate; // global in AudioWorklet scope
  }

  process(inputs, outputs) {
    const input = inputs[0] && inputs[0][0];
    if (!input || input.length === 0) return true;

    const ratio = this.nativeRate / this.targetRate;
    const outLen = Math.floor(input.length / ratio);
    const pcm = new Int16Array(outLen);

    for (let i = 0; i < outLen; i++) {
      const srcIdx = i * ratio;
      const idx = Math.floor(srcIdx);
      const frac = srcIdx - idx;
      const s0 = input[idx];
      const s1 = input[Math.min(idx + 1, input.length - 1)];
      const sample = s0 + frac * (s1 - s0);
      const clamped = Math.max(-1, Math.min(1, sample));
      pcm[i] = clamped < 0 ? clamped * 0x8000 : clamped * 0x7FFF;
    }

    this.port.postMessage(pcm.buffer, [pcm.buffer]);
    return true;
  }
}

registerProcessor("feline-capture", CaptureProcessor);
`;

const PLAYBACK_PROCESSOR_SRC = `
class PlaybackProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    this.serverRate = (options.processorOptions && options.processorOptions.serverSampleRate) || 24000;
    this.nativeRate = sampleRate;
    this.bufSize = this.nativeRate * 2; // 2 seconds ring buffer
    this.ring = new Float32Array(this.bufSize);
    this.writePos = 0;
    this.readPos = 0;

    this.port.onmessage = (ev) => {
      const pcm = new Int16Array(ev.data);
      const ratio = this.nativeRate / this.serverRate;
      const outLen = Math.round(pcm.length * ratio);

      for (let i = 0; i < outLen; i++) {
        const srcIdx = i / ratio;
        const idx = Math.floor(srcIdx);
        const frac = srcIdx - idx;
        const s0 = pcm[Math.min(idx, pcm.length - 1)];
        const s1 = pcm[Math.min(idx + 1, pcm.length - 1)];
        const raw = s0 + frac * (s1 - s0);
        const sample = raw / (raw < 0 ? 0x8000 : 0x7FFF);
        this.ring[this.writePos % this.bufSize] = sample;
        this.writePos++;
      }
    };
  }

  process(inputs, outputs) {
    const output = outputs[0] && outputs[0][0];
    if (!output) return true;

    for (let i = 0; i < output.length; i++) {
      if (this.readPos < this.writePos) {
        output[i] = this.ring[this.readPos % this.bufSize];
        this.readPos++;
      } else {
        output[i] = 0;
      }
    }
    return true;
  }
}

registerProcessor("feline-playback", PlaybackProcessor);
`;

function createBlobURL(source) {
  const blob = new Blob([source], { type: "application/javascript" });
  return URL.createObjectURL(blob);
}

// ---- Transport class ----

export class FelineWebSocketTransport {
  constructor(options = {}) {
    this._wsUrl = options.wsUrl || "ws://localhost:8765/ws";
    this._inputSampleRate = options.inputSampleRate || 16000;
    this._outputSampleRate = options.outputSampleRate || 24000;

    this._state = "disconnected";
    this._ws = null;
    this._micStream = null;
    this._micEnabled = true;
    this._captureCtx = null;
    this._captureWorklet = null;
    this._playbackCtx = null;
    this._playbackWorklet = null;
    this._playbackDest = null;
    this._localAudioTrack = null;
    this._botAudioTrack = null;
    this._selectedMic = {};
    this._selectedSpeaker = {};
    this._onMessage = null;
    this._callbacks = {};
    this._options = null;
    this._abortController = undefined;
    this._maxMessageSize = 64 * 1024;
  }

  // ---- Lifecycle ----

  initialize(options, messageHandler) {
    this._options = options;
    this._onMessage = messageHandler;
    this._callbacks = options.callbacks || {};
    this.state = "disconnected";
  }

  async initDevices() {
    this.state = "initializing";
    try {
      this._micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      this._localAudioTrack = this._micStream.getAudioTracks()[0];
      this.state = "initialized";
    } catch (err) {
      this.state = "error";
      throw err;
    }
  }

  _validateConnectionParams(connectParams) {
    return connectParams;
  }

  async connect(connectParams) {
    this._abortController = new AbortController();
    const validated = this._validateConnectionParams(connectParams);
    return this._connect(validated);
  }

  async _connect() {
    this.state = "connecting";

    // Determine WebSocket URL
    let wsUrl = this._wsUrl;
    if (wsUrl.startsWith("/")) {
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      wsUrl = `${proto}//${location.host}${wsUrl}`;
    }

    return new Promise((resolve, reject) => {
      this._ws = new WebSocket(wsUrl);
      this._ws.binaryType = "arraybuffer";

      this._ws.onopen = async () => {
        try {
          await this._setupAudio();
          this.state = "connected";
          this._callbacks.onConnected?.();
          resolve();
        } catch (err) {
          this.state = "error";
          reject(err);
        }
      };

      this._ws.onmessage = (ev) => {
        if (ev.data instanceof ArrayBuffer) {
          // Binary audio from server
          if (this._playbackWorklet) {
            this._playbackWorklet.port.postMessage(ev.data, [ev.data]);
          }
        } else {
          // Text JSON (RTVI message)
          try {
            const msg = JSON.parse(ev.data);
            if (this._onMessage) this._onMessage(msg);
          } catch (_) {
            // ignore malformed JSON
          }
        }
      };

      this._ws.onclose = () => {
        this._callbacks.onDisconnected?.();
        this.state = "disconnected";
      };

      this._ws.onerror = (err) => {
        this._callbacks.onError?.(err);
        if (this._state === "connecting") {
          reject(new Error("WebSocket connection failed"));
        }
      };
    });
  }

  async _disconnect() {
    this.state = "disconnecting";

    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }

    this._teardownAudio();
    this.state = "disconnected";
  }

  async disconnect() {
    if (this._abortController) {
      this._abortController.abort();
    }
    return this._disconnect();
  }

  sendReadyMessage() {
    this.state = "ready";
    // Send client-ready RTVI message
    const msg = {
      id: this._shortId(),
      label: "rtvi-ai",
      type: "client-ready",
      data: {
        version: "1.2.0",
        about: { library: "feline-transport", library_version: "0.1.0" },
      },
    };
    this.sendMessage(msg);
  }

  sendMessage(message) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(message));
    }
  }

  get maxMessageSize() {
    return this._maxMessageSize;
  }

  // ---- Audio setup/teardown ----

  async _setupAudio() {
    // Capture pipeline
    this._captureCtx = new AudioContext({ sampleRate: 48000 });
    const captureUrl = createBlobURL(CAPTURE_PROCESSOR_SRC);
    await this._captureCtx.audioWorklet.addModule(captureUrl);
    URL.revokeObjectURL(captureUrl);

    const source = this._captureCtx.createMediaStreamSource(this._micStream);
    this._captureWorklet = new AudioWorkletNode(this._captureCtx, "feline-capture", {
      processorOptions: { targetSampleRate: this._inputSampleRate },
    });

    this._captureWorklet.port.onmessage = (ev) => {
      if (this._ws?.readyState === WebSocket.OPEN && this._micEnabled) {
        this._ws.send(ev.data);
      }
    };

    source.connect(this._captureWorklet);
    // Don't connect to destination — we don't want local playback of mic

    // Playback pipeline
    this._playbackCtx = new AudioContext({ sampleRate: 48000 });
    const playbackUrl = createBlobURL(PLAYBACK_PROCESSOR_SRC);
    await this._playbackCtx.audioWorklet.addModule(playbackUrl);
    URL.revokeObjectURL(playbackUrl);

    this._playbackWorklet = new AudioWorkletNode(this._playbackCtx, "feline-playback", {
      processorOptions: { serverSampleRate: this._outputSampleRate },
    });

    this._playbackDest = this._playbackCtx.createMediaStreamDestination();
    this._playbackWorklet.connect(this._playbackDest);
    // Also connect to speakers directly
    this._playbackWorklet.connect(this._playbackCtx.destination);

    this._botAudioTrack = this._playbackDest.stream.getAudioTracks()[0];

    // Fire track callbacks
    if (this._localAudioTrack) {
      this._callbacks.onTrackStarted?.(this._localAudioTrack, {
        id: "local",
        name: "local",
        local: true,
      });
    }
    if (this._botAudioTrack) {
      this._callbacks.onTrackStarted?.(this._botAudioTrack, {
        id: "bot",
        name: "bot",
        local: false,
      });
    }
  }

  _teardownAudio() {
    if (this._captureWorklet) {
      this._captureWorklet.disconnect();
      this._captureWorklet = null;
    }
    if (this._captureCtx) {
      this._captureCtx.close().catch(() => {});
      this._captureCtx = null;
    }
    if (this._playbackWorklet) {
      this._playbackWorklet.disconnect();
      this._playbackWorklet = null;
    }
    if (this._playbackDest) {
      this._playbackDest = null;
    }
    if (this._playbackCtx) {
      this._playbackCtx.close().catch(() => {});
      this._playbackCtx = null;
    }
    if (this._micStream) {
      this._micStream.getTracks().forEach((t) => t.stop());
      this._micStream = null;
    }
    this._localAudioTrack = null;
    this._botAudioTrack = null;
  }

  // ---- Device management ----

  async getAllMics() {
    const devices = await navigator.mediaDevices.enumerateDevices();
    return devices.filter((d) => d.kind === "audioinput");
  }

  async getAllCams() {
    return [];
  }

  async getAllSpeakers() {
    const devices = await navigator.mediaDevices.enumerateDevices();
    return devices.filter((d) => d.kind === "audiooutput");
  }

  async updateMic(micId) {
    // Stop old stream
    if (this._micStream) {
      this._micStream.getTracks().forEach((t) => t.stop());
    }
    this._micStream = await navigator.mediaDevices.getUserMedia({
      audio: { deviceId: { exact: micId } },
    });
    this._localAudioTrack = this._micStream.getAudioTracks()[0];

    // Reconnect capture pipeline
    if (this._captureCtx && this._captureWorklet) {
      const source = this._captureCtx.createMediaStreamSource(this._micStream);
      source.connect(this._captureWorklet);
    }

    this._callbacks.onMicUpdated?.(this._micStream.getAudioTracks()[0]);
  }

  updateCam() {}
  updateSpeaker(speakerId) {
    this._selectedSpeaker = { deviceId: speakerId };
    this._callbacks.onSpeakerUpdated?.(speakerId);
  }

  get selectedMic() {
    return this._selectedMic;
  }
  get selectedCam() {
    return {};
  }
  get selectedSpeaker() {
    return this._selectedSpeaker;
  }

  enableMic(enable) {
    this._micEnabled = enable;
    if (this._localAudioTrack) {
      this._localAudioTrack.enabled = enable;
    }
  }

  enableCam() {}
  enableScreenShare() {}

  get isCamEnabled() {
    return false;
  }
  get isMicEnabled() {
    return this._micEnabled;
  }
  get isSharingScreen() {
    return false;
  }

  tracks() {
    return {
      local: { audio: this._localAudioTrack || undefined, video: undefined },
      bot: { audio: this._botAudioTrack || undefined, video: undefined },
    };
  }

  // ---- State management ----

  get state() {
    return this._state;
  }

  set state(newState) {
    if (this._state === newState) return;
    this._state = newState;
    this._callbacks.onTransportStateChanged?.(newState);
  }

  // Needed by PipecatClient for startBot flow
  get startBotParams() {
    return this._startBotParams;
  }

  set startBotParams(params) {
    this._startBotParams = params;
  }

  // ---- Helpers ----

  _shortId() {
    const arr = new Uint8Array(4);
    crypto.getRandomValues(arr);
    return Array.from(arr, (b) => b.toString(16).padStart(2, "0")).join("");
  }
}

export default FelineWebSocketTransport;
