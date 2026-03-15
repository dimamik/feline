defmodule Feline.Transports.Boombox.BrowserLogger do
  @moduledoc """
  Processor that sends user transcriptions and bot responses
  to the browser via a text WebSocket channel.
  """
  use Feline.Processor

  alias Feline.Frames.{
    TranscriptionFrame,
    LLMTextFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame
  }

  @impl true
  def init(opts) do
    {:ok, %{registry: Keyword.fetch!(opts, :registry)}}
  end

  @impl true
  def handle_frame(%TranscriptionFrame{text: text} = frame, dir, _push_fn, state) do
    send_to_browser(state, %{type: "user", text: text})
    {:push, frame, dir, state}
  end

  def handle_frame(%LLMFullResponseStartFrame{} = frame, dir, _push_fn, state) do
    send_to_browser(state, %{type: "bot_start"})
    {:push, frame, dir, state}
  end

  def handle_frame(%LLMTextFrame{text: text} = frame, dir, _push_fn, state) do
    send_to_browser(state, %{type: "bot_token", text: text})
    {:push, frame, dir, state}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, dir, _push_fn, state) do
    send_to_browser(state, %{type: "bot_end"})
    {:push, frame, dir, state}
  end

  def handle_frame(frame, dir, _push_fn, state) do
    {:push, frame, dir, state}
  end

  defp send_to_browser(state, payload) do
    case Agent.get(state.registry, & &1) do
      pid when is_pid(pid) -> send(pid, {:text_message, payload})
      _ -> :ok
    end
  end
end
