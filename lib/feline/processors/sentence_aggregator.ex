defmodule Feline.Processors.SentenceAggregator do
  @moduledoc """
  Buffers LLM token stream and emits complete sentences.

  Collects `LLMTextFrame` chunks until a sentence boundary (`.`, `!`, `?`)
  is found, then pushes each complete sentence as a `TextFrame`. Remaining
  text is flushed on `LLMFullResponseEndFrame` or `EndFrame`.
  """
  use Feline.Processor

  alias Feline.Frames.{
    LLMTextFrame,
    TextFrame,
    LLMFullResponseEndFrame,
    InterruptionFrame,
    EndFrame
  }

  @sentence_end_pattern ~r/[.!?]\s*/

  @impl true
  def init(opts) do
    {:ok,
     %{
       buffer: "",
       pattern: Keyword.get(opts, :pattern, @sentence_end_pattern)
     }}
  end

  @impl true
  def handle_frame(%LLMTextFrame{text: text}, :downstream, push_fn, state) do
    buffer = state.buffer <> text
    {sentences, rest} = extract_all_sentences(buffer, state.pattern, [])

    for sentence <- sentences do
      push_fn.(%TextFrame{id: make_ref(), text: sentence}, :downstream)
    end

    {:ok, %{state | buffer: rest}}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, direction, push_fn, state) do
    state = flush_buffer(state, push_fn)
    {:push, frame, direction, state}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, _push_fn, state) do
    {:push, frame, direction, %{state | buffer: ""}}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    state = flush_buffer(state, push_fn)
    {:push, frame, direction, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp extract_all_sentences(buffer, pattern, acc) do
    case Regex.run(pattern, buffer, return: :index) do
      [{pos, len}] ->
        split_at = pos + len
        sentence = String.slice(buffer, 0, split_at) |> String.trim()
        rest = String.slice(buffer, split_at..-1//1)
        extract_all_sentences(rest, pattern, [sentence | acc])

      _ ->
        {Enum.reverse(acc), buffer}
    end
  end

  defp flush_buffer(%{buffer: ""} = state, _push_fn), do: state

  defp flush_buffer(%{buffer: buffer} = state, push_fn) do
    text = String.trim(buffer)

    if text != "" do
      push_fn.(%TextFrame{id: make_ref(), text: text}, :downstream)
    end

    %{state | buffer: ""}
  end
end
