defmodule Feline.Transports.Boombox.CaptionSender do
  @moduledoc """
  Sends sentence text to the browser as captions when each sentence
  is ready for TTS. Placed just before the TTS processor.
  """
  use Feline.Processor

  alias Feline.Frames.TextFrame

  @impl true
  def init(opts) do
    {:ok, %{text_registry: Keyword.fetch!(opts, :text_registry)}}
  end

  @impl true
  def handle_frame(%TextFrame{text: text} = frame, dir, _push_fn, state) do
    case Agent.get(state.text_registry, & &1) do
      pid when is_pid(pid) -> send(pid, {:text_message, %{type: "caption", text: text}})
      _ -> :ok
    end

    {:push, frame, dir, state}
  end

  def handle_frame(frame, dir, _push_fn, state) do
    {:push, frame, dir, state}
  end
end
