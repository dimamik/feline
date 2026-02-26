defmodule Feline.Processors.AssistantContextAggregator do
  @moduledoc """
  Accumulates LLM response text into the conversation context.
  Collects text between LLMFullResponseStartFrame and LLMFullResponseEndFrame.

  Accepts either a `context_agent` (shared via ContextAggregatorPair)
  or a standalone `context` option.
  """
  use Feline.Processor

  alias Feline.Context
  alias Feline.Processors.ContextAggregatorPair

  alias Feline.Frames.{
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    LLMTextFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       context_agent: Keyword.get(opts, :context_agent),
       context: Keyword.get(opts, :context, Context.new()),
       accumulating: false,
       pending_text: ""
     }}
  end

  @impl true
  def handle_frame(%LLMFullResponseStartFrame{} = frame, :downstream, _push_fn, state) do
    {:push, frame, :downstream, %{state | accumulating: true, pending_text: ""}}
  end

  def handle_frame(%LLMTextFrame{text: text} = frame, :downstream, _push_fn, state) do
    if state.accumulating do
      {:push, frame, :downstream, %{state | pending_text: state.pending_text <> text}}
    else
      {:push, frame, :downstream, state}
    end
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, :downstream, _push_fn, state) do
    if state.accumulating and String.trim(state.pending_text) != "" do
      message = %{role: "assistant", content: String.trim(state.pending_text)}
      context = append_and_get_context(state, message)

      {:push, frame, :downstream,
       %{state | context: context, accumulating: false, pending_text: ""}}
    else
      {:push, frame, :downstream, %{state | accumulating: false, pending_text: ""}}
    end
  end

  def handle_frame(%FunctionCallInProgressFrame{} = frame, :downstream, _push_fn, state) do
    message = %{
      role: "assistant",
      tool_calls: [
        %{
          id: frame.tool_call_id,
          type: "function",
          function: %{name: frame.function_name, arguments: frame.arguments}
        }
      ]
    }

    context = append_and_get_context(state, message)
    {:push, frame, :downstream, %{state | context: context}}
  end

  def handle_frame(%FunctionCallResultFrame{} = frame, :downstream, _push_fn, state) do
    message = %{role: "tool", tool_call_id: frame.tool_call_id, content: frame.result}
    context = append_and_get_context(state, message)
    {:push, frame, :downstream, %{state | context: context}}
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
