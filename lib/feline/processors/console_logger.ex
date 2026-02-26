defmodule Feline.Processors.ConsoleLogger.UserInput do
  @moduledoc """
  Logs user transcriptions and speaking state to the console.
  Place before UserContextAggregator (which absorbs TranscriptionFrame).
  """
  use Feline.Processor

  alias Feline.Frames.{
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame
  }

  @listening_label "[listening...]"

  @impl true
  def init(_opts), do: {:ok, %{listening: false}}

  @impl true
  def handle_frame(%UserStartedSpeakingFrame{} = frame, dir, _push_fn, state) do
    IO.write(IO.ANSI.faint() <> @listening_label <> IO.ANSI.reset())
    {:push, frame, dir, %{state | listening: true}}
  end

  def handle_frame(%UserStoppedSpeakingFrame{} = frame, dir, _push_fn, state) do
    clear_listening(state)
    {:push, frame, dir, %{state | listening: false}}
  end

  def handle_frame(%TranscriptionFrame{text: text} = frame, dir, _push_fn, state) do
    clear_listening(state)
    IO.puts(IO.ANSI.cyan() <> "You: " <> IO.ANSI.reset() <> text)
    {:push, frame, dir, %{state | listening: false}}
  end

  def handle_frame(frame, dir, _push_fn, state) do
    {:push, frame, dir, state}
  end

  defp clear_listening(%{listening: true}) do
    IO.write("\r" <> String.duplicate(" ", String.length(@listening_label)) <> "\r")
  end

  defp clear_listening(_state), do: :ok
end

defmodule Feline.Processors.ConsoleLogger.BotOutput do
  @moduledoc """
  Logs bot LLM responses to the console, streaming token by token.
  Place after AssistantContextAggregator.
  """
  use Feline.Processor

  alias Feline.Frames.{
    LLMTextFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame
  }

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_frame(%LLMFullResponseStartFrame{} = frame, dir, _push_fn, state) do
    IO.write(IO.ANSI.green() <> "Bot: " <> IO.ANSI.reset())
    {:push, frame, dir, state}
  end

  def handle_frame(%LLMTextFrame{text: text} = frame, dir, _push_fn, state) do
    IO.write(text)
    {:push, frame, dir, state}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, dir, _push_fn, state) do
    IO.puts("")
    {:push, frame, dir, state}
  end

  def handle_frame(frame, dir, _push_fn, state) do
    {:push, frame, dir, state}
  end
end
