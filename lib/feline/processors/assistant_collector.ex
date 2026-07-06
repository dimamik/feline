defmodule Feline.Processors.AssistantCollector do
  @moduledoc """
  Collects the LLM's streamed text and commits it to the context owner
  (upstream `LLMMessagesAppendFrame`) when the response completes - or
  partially when an interruption cuts it off, so the context reflects what
  the bot actually got to say. Place downstream of the LLM service.
  """

  # ponytail: commits the full generated text on interruption, not the
  # spoken-so-far prefix; word-timestamp accounting can refine this later.

  use Feline.Processor

  alias Feline.Frames.All.{
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMMessagesAppendFrame,
    LLMTextFrame
  }

  @impl true
  def init(_opts), do: {:ok, %{parts: nil}}

  @impl true
  def handle_frame(%LLMFullResponseStartFrame{} = frame, :downstream, _ctx, state) do
    {:push, frame, :downstream, %{state | parts: []}}
  end

  def handle_frame(%LLMTextFrame{} = frame, :downstream, _ctx, %{parts: parts} = state)
      when is_list(parts) do
    {:push, frame, :downstream, %{state | parts: [frame.text | parts]}}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, :downstream, _ctx, state) do
    case commit(state) do
      {nil, state} -> {:push, frame, :downstream, state}
      {append, state} -> {:push_many, [{frame, :downstream}, {append, :upstream}], state}
    end
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, _ctx, state) do
    case commit(state) do
      {nil, state} -> {:push, frame, direction, state}
      {append, state} -> {:push_many, [{frame, direction}, {append, :upstream}], state}
    end
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp commit(%{parts: parts} = state) when is_list(parts) and parts != [] do
    text = parts |> Enum.reverse() |> Enum.join()

    append = %LLMMessagesAppendFrame{
      messages: [%{"role" => "assistant", "content" => text}]
    }

    {append, %{state | parts: nil}}
  end

  defp commit(state), do: {nil, %{state | parts: nil}}
end
