if Code.ensure_loaded?(Phoenix.Endpoint) do
  defmodule Mix.Tasks.Feline.Dev do
    @moduledoc """
    Starts a Phoenix LiveView demo app with the Feline VoiceBot component.

    Opens a browser-based voice assistant that uses the RTVI protocol over
    WebSocket. Audio is captured from the mic, sent as raw PCM, processed
    through a Feline pipeline (STT → LLM → TTS), and played back.

    Requires Deepgram STT, OpenAI LLM, and Deepgram TTS.

    ## Environment variables (or .env file)

        OPENAI_API_KEY=...
        DEEPGRAM_API_KEY=...

    ## Usage

        mix feline.dev
        mix feline.dev --port 4000
        mix feline.dev --system "You are a pirate. Respond in pirate speak."
    """
    use Mix.Task

    @impl Mix.Task
    def run(args) do
      Mix.Task.run("app.start")
      load_dotenv()

      {opts, _} = OptionParser.parse!(args, strict: [system: :string, port: :integer])

      port = Keyword.get(opts, :port, 4000)

      system_prompt =
        Keyword.get(opts, :system, "You are a helpful voice assistant. Keep responses brief.")

      # Validate env vars early
      openai_key = require_env!("OPENAI_API_KEY")
      deepgram_key = require_env!("DEEPGRAM_API_KEY")

      # Store config for the pipeline builder
      Application.put_env(:feline_dev, :pipeline_config, %{
        system_prompt: system_prompt,
        openai_key: openai_key,
        deepgram_key: deepgram_key
      })

      # Configure the endpoint
      Application.put_env(:feline_dev, Feline.Dev.Endpoint,
        adapter: Bandit.PhoenixAdapter,
        http: [port: port],
        server: true,
        live_view: [signing_salt: "feline_dev_salt"],
        secret_key_base: String.duplicate("feline_dev_secret_key_base_", 3),
        pubsub_server: Feline.Dev.PubSub,
        render_errors: [formats: [html: Feline.Dev.ErrorHTML], layout: false]
      )

      # Start PubSub (required by LiveView)
      {:ok, _} =
        Supervisor.start_link(
          [{Phoenix.PubSub, name: Feline.Dev.PubSub}],
          strategy: :one_for_one
        )

      # Start the endpoint
      {:ok, _} = Feline.Dev.Endpoint.start_link()

      Mix.shell().info("Feline RTVI voice assistant running.")
      Mix.shell().info("Open http://localhost:#{port} in your browser and click Start.")
      Mix.shell().info("Press Ctrl+C to stop.\n")

      Process.sleep(:infinity)
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

  # ---- Inline Phoenix app modules ----
  # Router must be defined before Endpoint (Endpoint references Router at compile time)

  defmodule Feline.Dev.Router do
    use Phoenix.Router
    import Phoenix.LiveView.Router

    pipeline :browser do
      plug(:accepts, ["html"])
      plug(:fetch_session)
      plug(:put_root_layout, html: {Feline.Dev.Layouts, :root})
      plug(:protect_from_forgery)
      plug(:put_secure_browser_headers)
    end

    scope "/" do
      pipe_through(:browser)

      live("/", Feline.Dev.VoiceLive)
    end

    # WebSocket endpoint for RTVI audio + messages
    forward("/feline", Feline.Phoenix.RTVIPlug,
      pipeline_builder: &Feline.Dev.PipelineBuilder.build/1
    )
  end

  defmodule Feline.Dev.Endpoint do
    use Phoenix.Endpoint, otp_app: :feline_dev

    socket("/live", Phoenix.LiveView.Socket)

    # Serve LiveView JS from deps
    plug(Plug.Static,
      at: "/assets/phoenix",
      from: {:phoenix, "priv/static"},
      only: ~w(phoenix.min.js)
    )

    plug(Plug.Static,
      at: "/assets/phoenix_live_view",
      from: {:phoenix_live_view, "priv/static"},
      only: ~w(phoenix_live_view.js)
    )

    plug(Plug.Session,
      store: :cookie,
      key: "_feline_dev_key",
      signing_salt: "feline_dev"
    )

    plug(Plug.Parsers,
      parsers: [:urlencoded, :multipart, :json],
      pass: ["*/*"],
      json_decoder: Jason
    )

    plug(Feline.Dev.Router)
  end

  defmodule Feline.Dev.PipelineBuilder do
    def build(ws_pid) do
      config = Application.get_env(:feline_dev, :pipeline_config)

      context =
        Feline.Context.new([%{"role" => "system", "content" => config.system_prompt}])

      {:ok, pair} = Feline.Processors.ContextAggregatorPair.start(context)

      Feline.Pipeline.new([
        {Feline.RTVI.Processor, []},
        {Feline.Processors.VADProcessor, start_secs: 0.2, stop_secs: 0.8},
        {Feline.Services.Deepgram.StreamingSTT,
         api_key: config.deepgram_key, sample_rate: 16_000},
        {Feline.Processors.UserContextAggregator, context_agent: pair.agent},
        {Feline.Services.OpenAI.StreamingLLM, api_key: config.openai_key, model: "gpt-4.1-mini"},
        {Feline.Processors.AssistantContextAggregator, context_agent: pair.agent},
        {Feline.Processors.SentenceAggregator, []},
        {Feline.RTVI.EventEmitter, []},
        {Feline.Services.Deepgram.StreamingTTS,
         api_key: config.deepgram_key, sample_rate: 24_000},
        {Feline.Transports.WebSocket.Output,
         ws_pid: ws_pid,
         rtvi_enabled: true,
         params: %Feline.TransportParams{audio_out_sample_rate: 24_000}}
      ])
    end
  end

  defmodule Feline.Dev.ErrorHTML do
    def render(_template, _assigns), do: "Not Found"
  end

  defmodule Feline.Dev.Layouts do
    use Phoenix.Component

    def root(assigns) do
      ~H"""
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta name="csrf-token" content={Phoenix.Controller.get_csrf_token()} />
        <title>Feline Voice Assistant</title>
        <style>
          * { box-sizing: border-box; margin: 0; padding: 0; }
          body {
            font-family: system-ui, -apple-system, sans-serif;
            background: #1a1a2e;
            color: #e0e0e0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
          }
          .container { text-align: center; max-width: 600px; width: 100%; padding: 2rem; }
          h1 { font-size: 1.5rem; margin-bottom: 0.5rem; color: #fff; }
          .subtitle { color: #888; margin-bottom: 2rem; font-size: 0.9rem; }
          .btn {
            background: #4a90d9; color: white; border: none;
            padding: 1rem 2.5rem; font-size: 1.1rem; border-radius: 2rem;
            cursor: pointer; transition: background 0.2s;
          }
          .btn:hover { background: #357abd; }
          .btn:disabled { background: #555; cursor: not-allowed; }
          .btn-stop { background: #d94a4a; }
          .btn-stop:hover { background: #bd3535; }
          .status { margin-top: 1.5rem; font-size: 0.9rem; color: #aaa; }
          .status.connected { color: #4caf50; }
          .status.error { color: #f44336; }
          .indicators { margin-top: 1rem; }
          .dot {
            display: inline-block; width: 10px; height: 10px;
            border-radius: 50%; background: #555; margin-right: 6px; vertical-align: middle;
            transition: background 0.2s;
          }
          .dot.active { background: #4caf50; }
          .dot.speaking { background: #ff9800; }
          .transcript {
            margin-top: 2rem; text-align: left; background: #16213e;
            border-radius: 12px; padding: 1.5rem; min-height: 200px; max-height: 400px;
            overflow-y: auto; font-size: 0.9rem; line-height: 1.6;
          }
          .transcript:empty::before { content: 'Conversation will appear here...'; color: #555; }
          .msg { margin-bottom: 0.5rem; }
          .msg-user { color: #64b5f6; }
          .msg-user::before { content: 'You: '; font-weight: 600; }
          .msg-bot { color: #81c784; }
          .msg-bot::before { content: 'Bot: '; font-weight: 600; }
          .msg-system { color: #888; font-style: italic; font-size: 0.8rem; }
        </style>
        <script>
          // ---- Inline AudioWorklet processors ----
          const CAPTURE_SRC = `
            class C extends AudioWorkletProcessor {
              constructor(o) { super(); this.r = sampleRate / ((o.processorOptions && o.processorOptions.rate) || 16000); }
              process(inputs) {
                const d = inputs[0] && inputs[0][0];
                if (!d || !d.length) return true;
                const n = Math.floor(d.length / this.r), p = new Int16Array(n);
                for (let i = 0; i < n; i++) {
                  const si = i * this.r, idx = Math.floor(si), f = si - idx;
                  const s = d[idx] + f * (d[Math.min(idx+1,d.length-1)] - d[idx]);
                  const c = Math.max(-1, Math.min(1, s));
                  p[i] = c < 0 ? c * 0x8000 : c * 0x7FFF;
                }
                this.port.postMessage(p.buffer, [p.buffer]);
                return true;
              }
            }
            registerProcessor("cap", C);`;

          const PLAYBACK_SRC = `
            class P extends AudioWorkletProcessor {
              constructor(o) {
                super();
                this.sr = (o.processorOptions && o.processorOptions.rate) || 24000;
                this.nr = sampleRate;
                this.bs = this.nr * 2;
                this.ring = new Float32Array(this.bs);
                this.wp = 0; this.rp = 0;
                this.port.onmessage = (e) => {
                  const pcm = new Int16Array(e.data);
                  const ratio = this.nr / this.sr;
                  const n = Math.round(pcm.length * ratio);
                  for (let i = 0; i < n; i++) {
                    const si = i / ratio, idx = Math.floor(si), f = si - idx;
                    const s0 = pcm[Math.min(idx, pcm.length-1)];
                    const s1 = pcm[Math.min(idx+1, pcm.length-1)];
                    const raw = s0 + f * (s1 - s0);
                    this.ring[this.wp % this.bs] = raw / (raw < 0 ? 0x8000 : 0x7FFF);
                    this.wp++;
                  }
                };
              }
              process(_, outputs) {
                const out = outputs[0] && outputs[0][0];
                if (!out) return true;
                for (let i = 0; i < out.length; i++) {
                  out[i] = this.rp < this.wp ? this.ring[this.rp++ % this.bs] : 0;
                }
                return true;
              }
            }
            registerProcessor("play", P);`;

          function blobUrl(src) {
            return URL.createObjectURL(new Blob([src], { type: 'application/javascript' }));
          }
          function shortId() {
            const a = new Uint8Array(4); crypto.getRandomValues(a);
            return Array.from(a, b => b.toString(16).padStart(2, '0')).join('');
          }

          // ---- FelineVoiceBot LiveView Hook ----
          window.FelineVoiceBot = {
            mounted() {
              this.ws = null;
              this.micStream = null;
              this.capCtx = null;
              this.capNode = null;
              this.playCtx = null;
              this.playNode = null;
              this.running = false;

              this.handleEvent("feline:connect", () => this.start());
              this.handleEvent("feline:disconnect", () => this.stop());
            },

            destroyed() { this.stop(); },

            async start() {
              if (this.running) return;
              const wsUrl = this.el.dataset.wsUrl || "/feline/ws";
              const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
              const fullUrl = wsUrl.startsWith('/') ? proto + '//' + location.host + wsUrl : wsUrl;

              try {
                this.micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
              } catch (e) {
                this.pushEvent("feline:error", { message: "Microphone access denied" });
                return;
              }

              this.ws = new WebSocket(fullUrl);
              this.ws.binaryType = 'arraybuffer';

              this.ws.onopen = async () => {
                try {
                  await this.setupAudio();
                  this.running = true;
                  this.pushEvent("feline:connected", {});

                  this.ws.send(JSON.stringify({
                    id: shortId(), label: 'rtvi-ai', type: 'client-ready',
                    data: { version: '1.2.0', about: { library: 'feline-dev' } }
                  }));
                } catch (e) {
                  this.pushEvent("feline:error", { message: "Audio setup failed: " + e.message });
                  this.cleanup();
                }
              };

              this.ws.onmessage = (ev) => {
                if (ev.data instanceof ArrayBuffer) {
                  if (this.playNode) this.playNode.port.postMessage(ev.data, [ev.data]);
                } else {
                  try {
                    const msg = JSON.parse(ev.data);
                    this.pushEvent("feline:rtvi", msg);
                  } catch (_) {}
                }
              };

              this.ws.onclose = () => {
                if (this.running) {
                  this.cleanup();
                  this.pushEvent("feline:disconnected", {});
                }
              };

              this.ws.onerror = () => {
                this.pushEvent("feline:error", { message: "WebSocket error" });
                this.cleanup();
              };
            },

            stop() {
              if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.ws.send(JSON.stringify({
                  id: shortId(), label: 'rtvi-ai', type: 'disconnect-bot', data: {}
                }));
              }
              this.cleanup();
            },

            cleanup() {
              this.running = false;
              if (this.capNode) { this.capNode.disconnect(); this.capNode = null; }
              if (this.capCtx) { this.capCtx.close().catch(()=>{}); this.capCtx = null; }
              if (this.playNode) { this.playNode.disconnect(); this.playNode = null; }
              if (this.playCtx) { this.playCtx.close().catch(()=>{}); this.playCtx = null; }
              if (this.micStream) { this.micStream.getTracks().forEach(t => t.stop()); this.micStream = null; }
              if (this.ws) { this.ws.close(); this.ws = null; }
            },

            async setupAudio() {
              this.capCtx = new AudioContext({ sampleRate: 48000 });
              const cu = blobUrl(CAPTURE_SRC);
              await this.capCtx.audioWorklet.addModule(cu);
              URL.revokeObjectURL(cu);
              const src = this.capCtx.createMediaStreamSource(this.micStream);
              this.capNode = new AudioWorkletNode(this.capCtx, 'cap', { processorOptions: { rate: 16000 } });
              this.capNode.port.onmessage = (ev) => {
                if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(ev.data);
              };
              src.connect(this.capNode);

              this.playCtx = new AudioContext({ sampleRate: 48000 });
              const pu = blobUrl(PLAYBACK_SRC);
              await this.playCtx.audioWorklet.addModule(pu);
              URL.revokeObjectURL(pu);
              this.playNode = new AudioWorkletNode(this.playCtx, 'play', { processorOptions: { rate: 24000 } });
              this.playNode.connect(this.playCtx.destination);
            }
          };
        </script>
      </head>
      <body>
        <%= @inner_content %>
        <script src="/assets/phoenix/phoenix.min.js"></script>
        <script src="/assets/phoenix_live_view/phoenix_live_view.js"></script>
        <script>
          let liveSocket = new LiveView.LiveSocket("/live", Phoenix.Socket, {
            hooks: { FelineVoiceBot: window.FelineVoiceBot }
          });
          liveSocket.connect();
        </script>
      </body>
      </html>
      """
    end
  end

  defmodule Feline.Dev.VoiceLive do
    use Phoenix.LiveView
    import Feline.Phoenix.VoiceBot

    @impl true
    def mount(_params, _session, socket) do
      {:ok,
       assign(socket,
         status: :idle,
         messages: [],
         bot_text_acc: ""
       )}
    end

    @impl true
    def render(assigns) do
      ~H"""
      <div class="container">
        <h1>Feline Voice Assistant</h1>
        <p class="subtitle">RTVI protocol · LiveView + WebSocket PCM audio</p>

        <.voice_bot id="voice-bot" ws_url="/feline/ws">
          <%= if @status == :idle do %>
            <button class="btn" phx-click="start">Start</button>
          <% else %>
            <button class="btn btn-stop" phx-click="stop">Stop</button>
          <% end %>
        </.voice_bot>

        <div class={"status #{status_class(@status)}"}>
          <%= status_text(@status) %>
        </div>

        <div class="indicators">
          <span class={"dot #{if @status in [:connected, :ready], do: "active"}"} />
          Mic
          &nbsp;&nbsp;
          <span class={"dot #{if @status == :ready, do: "active"}"} />
          Bot
        </div>

        <div class="transcript">
          <%= for msg <- @messages do %>
            <div class={"msg msg-#{msg.role}"}>
              <%= msg.text %>
            </div>
          <% end %>
        </div>
      </div>
      """
    end

    @impl true
    def handle_event("start", _params, socket) do
      socket =
        socket
        |> assign(status: :connecting)
        |> push_event("feline:connect", %{})

      {:noreply, socket}
    end

    def handle_event("stop", _params, socket) do
      socket =
        socket
        |> assign(status: :idle, bot_text_acc: "")
        |> push_event("feline:disconnect", %{})

      {:noreply, socket}
    end

    def handle_event("feline:connected", _params, socket) do
      {:noreply, assign(socket, status: :connected)}
    end

    def handle_event("feline:disconnected", _params, socket) do
      {:noreply, assign(socket, status: :idle, bot_text_acc: "")}
    end

    def handle_event("feline:error", %{"message" => msg}, socket) do
      socket =
        socket
        |> assign(status: :idle)
        |> update(:messages, &(&1 ++ [%{role: :system, text: "Error: #{msg}"}]))

      {:noreply, socket}
    end

    def handle_event("feline:rtvi", %{"type" => type} = msg, socket) do
      handle_rtvi(type, msg["data"] || %{}, socket)
    end

    def handle_event(_event, _params, socket) do
      {:noreply, socket}
    end

    defp handle_rtvi("bot-ready", _data, socket) do
      socket =
        socket
        |> assign(status: :ready)
        |> update(
          :messages,
          &(&1 ++ [%{role: :system, text: "Bot connected — speak into your mic"}])
        )

      {:noreply, socket}
    end

    defp handle_rtvi("user-transcription", %{"text" => text, "final" => true}, socket) do
      {:noreply, update(socket, :messages, &(&1 ++ [%{role: :user, text: text}]))}
    end

    defp handle_rtvi("bot-llm-text", %{"text" => text}, socket) do
      acc = socket.assigns.bot_text_acc <> text
      {:noreply, assign(socket, bot_text_acc: acc)}
    end

    defp handle_rtvi("bot-llm-stopped", _data, socket) do
      if socket.assigns.bot_text_acc != "" do
        socket =
          socket
          |> update(:messages, &(&1 ++ [%{role: :bot, text: socket.assigns.bot_text_acc}]))
          |> assign(bot_text_acc: "")

        {:noreply, socket}
      else
        {:noreply, socket}
      end
    end

    defp handle_rtvi(_type, _data, socket) do
      {:noreply, socket}
    end

    defp status_class(:idle), do: ""
    defp status_class(:connecting), do: ""
    defp status_class(:connected), do: "connected"
    defp status_class(:ready), do: "connected"

    defp status_text(:idle), do: ""
    defp status_text(:connecting), do: "Connecting..."
    defp status_text(:connected), do: "Connected, waiting for bot..."
    defp status_text(:ready), do: "Ready — speak into your microphone"
  end
end
