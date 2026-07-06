defmodule Feline.Processors.ContextAggregator do
  @moduledoc """
  Owns the conversation context (there is exactly one owner per pipeline -
  no shared mutable object as in pipecat). Sits upstream of the LLM service.

  Downstream inputs: transcriptions aggregate into the current user turn;
  the turn commits and triggers the LLM (`LLMContextFrame`) when the user
  stops speaking - or immediately for a final transcription arriving after
  the turn ended (STT latency). `LLMMessagesAppendFrame` (e.g. RTVI
  `send-text`) appends directly.

  Upstream inputs (consumed here): `LLMMessagesAppendFrame` from the
  assistant collector commits bot responses; `FunctionCallResultFrame`
  appends the tool exchange and re-runs the LLM.

  Options: `:system_prompt`, `:tools` (forwarded to the LLM via the context),
  `:greeting` (assistant message committed and spoken on start).
  """

  use Feline.Processor

  alias Feline.LLM.Context

  alias Feline.Frames.All.{
    FunctionCallResultFrame,
    InterimTranscriptionFrame,
    LLMContextFrame,
    LLMMessagesAppendFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame
  }

  @impl true
  def init(opts) do
    system_messages =
      case Keyword.get(opts, :system_prompt) do
        nil -> []
        prompt -> [%{"role" => "system", "content" => prompt}]
      end

    {:ok,
     %{
       context: Context.new(messages: system_messages, tools: Keyword.get(opts, :tools, [])),
       greeting: Keyword.get(opts, :greeting),
       speaking?: false,
       aggregation: []
     }}
  end

  @impl true
  def handle_setup(_start_frame, ctx, %{greeting: greeting} = state) when is_binary(greeting) do
    context = Context.append(state.context, %{"role" => "assistant", "content" => greeting})
    ctx.push.(%Feline.Frames.All.TextFrame{text: greeting}, :downstream)
    {:ok, %{state | context: context}}
  end

  def handle_setup(_start_frame, _ctx, state), do: {:ok, state}

  @impl true
  def handle_frame(%UserStartedSpeakingFrame{} = frame, :downstream, _ctx, state) do
    {:push, frame, :downstream, %{state | speaking?: true}}
  end

  def handle_frame(%UserStoppedSpeakingFrame{} = frame, :downstream, _ctx, state) do
    state = %{state | speaking?: false}

    case commit_user_turn(state) do
      {nil, state} -> {:push, frame, :downstream, state}
      {run, state} -> {:push_many, [{frame, :downstream}, {run, :downstream}], state}
    end
  end

  def handle_frame(%TranscriptionFrame{} = frame, :downstream, _ctx, state) do
    state = %{state | aggregation: state.aggregation ++ [frame.text]}

    if state.speaking? do
      {:push, frame, :downstream, state}
    else
      case commit_user_turn(state) do
        {nil, state} -> {:push, frame, :downstream, state}
        {run, state} -> {:push_many, [{frame, :downstream}, {run, :downstream}], state}
      end
    end
  end

  def handle_frame(%InterimTranscriptionFrame{} = frame, :downstream, _ctx, state) do
    {:push, frame, :downstream, state}
  end

  def handle_frame(%LLMMessagesAppendFrame{} = frame, :downstream, _ctx, state) do
    context = Context.append(state.context, frame.messages)
    state = %{state | context: context}

    if frame.run_llm do
      {:push, %LLMContextFrame{context: context}, :downstream, state}
    else
      {:ok, state}
    end
  end

  def handle_frame(%LLMMessagesAppendFrame{} = frame, :upstream, _ctx, state) do
    {:ok, %{state | context: Context.append(state.context, frame.messages)}}
  end

  def handle_frame(%FunctionCallResultFrame{} = frame, _direction, _ctx, state) do
    tool_call = %{
      "id" => frame.tool_call_id,
      "type" => "function",
      "function" => %{
        "name" => frame.function_name,
        "arguments" => Jason.encode!(frame.arguments)
      }
    }

    context =
      Context.append(state.context, [
        %{"role" => "assistant", "content" => nil, "tool_calls" => [tool_call]},
        %{
          "role" => "tool",
          "tool_call_id" => frame.tool_call_id,
          "content" => Jason.encode!(frame.result)
        }
      ])

    state = %{state | context: context}
    {:push, %LLMContextFrame{context: context}, :downstream, state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp commit_user_turn(%{aggregation: []} = state), do: {nil, state}

  defp commit_user_turn(state) do
    text = state.aggregation |> Enum.join(" ") |> String.trim()
    context = Context.append(state.context, %{"role" => "user", "content" => text})
    state = %{state | context: context, aggregation: []}
    {%LLMContextFrame{context: context}, state}
  end
end
