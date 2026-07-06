defmodule Feline.Processors.SentenceAggregator do
  @moduledoc """
  Buffers streamed `LLMTextFrame` tokens and emits one `TextFrame` per
  complete sentence (all of them, not just the first - looping here matters
  for chunks containing several terminators). The remainder flushes on
  `LLMFullResponseEndFrame`; an interruption clears the buffer.

  `LLMTextFrame`s are also forwarded untouched so downstream reporters
  (RTVI `bot-llm-text`) still see the raw token stream.
  """

  use Feline.Processor

  alias Feline.Frames.All.{
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMTextFrame,
    TextFrame
  }

  @sentence_boundary ~r/(?<=[.!?:;])\s+/

  @impl true
  def init(_opts), do: {:ok, %{buffer: ""}}

  @impl true
  def handle_frame(%LLMTextFrame{} = frame, :downstream, _ctx, state) do
    {sentences, buffer} = extract_sentences(state.buffer <> frame.text)

    pushes =
      [{frame, :downstream}] ++
        for sentence <- sentences, do: {%TextFrame{text: sentence}, :downstream}

    {:push_many, pushes, %{state | buffer: buffer}}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, :downstream, _ctx, state) do
    case String.trim(state.buffer) do
      "" ->
        {:push, frame, :downstream, %{state | buffer: ""}}

      remainder ->
        {:push_many, [{%TextFrame{text: remainder}, :downstream}, {frame, :downstream}],
         %{state | buffer: ""}}
    end
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, _ctx, state) do
    {:push, frame, direction, %{state | buffer: ""}}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp extract_sentences(buffer) do
    case Regex.split(@sentence_boundary, buffer) do
      [incomplete] -> {[], incomplete}
      parts -> {Enum.slice(parts, 0..-2//1), List.last(parts)}
    end
  end
end
