defmodule Feline.Processors.UserContextAggregator do
  @moduledoc """
  Accumulates user transcriptions into the LLM context. When the user
  stops speaking (UserStoppedSpeakingFrame), pushes an LLMContextFrame
  to trigger LLM processing.

  Accepts either a `context_agent` (shared via ContextAggregatorPair)
  or a standalone `context` option.
  """
  use Feline.Processor

  alias Feline.Context
  alias Feline.Processors.ContextAggregatorPair

  alias Feline.Frames.{
    TranscriptionFrame,
    InterimTranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    LLMContextFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       context_agent: Keyword.get(opts, :context_agent),
       context: Keyword.get(opts, :context, Context.new()),
       pending_text: ""
     }}
  end

  @impl true
  def handle_frame(%TranscriptionFrame{text: text}, :downstream, _push_fn, state) do
    {:ok, %{state | pending_text: state.pending_text <> text}}
  end

  def handle_frame(%InterimTranscriptionFrame{}, :downstream, _push_fn, state) do
    {:ok, state}
  end

  def handle_frame(%UserStartedSpeakingFrame{} = frame, :downstream, _push_fn, state) do
    {:push, frame, :downstream, state}
  end

  def handle_frame(%UserStoppedSpeakingFrame{} = frame, :downstream, _push_fn, state) do
    if String.trim(state.pending_text) != "" do
      message = %{role: "user", content: String.trim(state.pending_text)}
      context = append_and_get_context(state, message)

      context_frame = %LLMContextFrame{
        id: make_ref(),
        context: context
      }

      {:push_many, [{frame, :downstream}, {context_frame, :downstream}],
       %{state | context: context, pending_text: ""}}
    else
      {:push, frame, :downstream, %{state | pending_text: ""}}
    end
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp append_and_get_context(%{context_agent: agent}, message) when is_pid(agent) do
    ContextAggregatorPair.append_message(agent, message)
  end

  defp append_and_get_context(%{context: context}, message) do
    Context.append_message(context, message)
  end
end
