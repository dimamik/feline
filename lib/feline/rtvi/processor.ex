defmodule Feline.RTVI.Processor do
  @moduledoc """
  Processor that handles inbound RTVI protocol messages.

  Place early in the pipeline (before STT/LLM/TTS). Intercepts
  `InputTransportMessageFrame` with `label: "rtvi-ai"` and converts
  them to the appropriate pipeline frames.

  Handled message types:
  - `client-ready` — triggers `bot-ready` response
  - `send-text` — pushes `LLMMessagesAppendFrame`
  - `disconnect-bot` — pushes `EndFrame`
  - `llm-function-call-result` — pushes `FunctionCallResultFrame`
  """
  use Feline.Processor

  alias Feline.Frame
  alias Feline.RTVI.Messages

  alias Feline.Frames.{
    EndFrame,
    FunctionCallResultFrame,
    InputTransportMessageFrame,
    LLMMessagesAppendFrame,
    OutputTransportMessageFrame,
    StartFrame
  }

  @rtvi_label "rtvi-ai"

  @impl true
  def init(_opts) do
    {:ok, %{pipeline_started: false, client_ready: false}}
  end

  @impl true
  def handle_frame(%StartFrame{} = frame, :downstream, push_fn, state) do
    state = %{state | pipeline_started: true}

    if state.client_ready do
      push_fn.(
        %OutputTransportMessageFrame{id: Frame.new_id(), payload: Messages.bot_ready()},
        :downstream
      )
    end

    {:push, frame, :downstream, state}
  end

  def handle_frame(
        %InputTransportMessageFrame{payload: %{"label" => @rtvi_label} = payload},
        :downstream,
        push_fn,
        state
      ) do
    handle_rtvi_message(payload["type"], payload, push_fn, state)
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp handle_rtvi_message("client-ready", _payload, push_fn, state) do
    state = %{state | client_ready: true}

    if state.pipeline_started do
      push_fn.(
        %OutputTransportMessageFrame{id: Frame.new_id(), payload: Messages.bot_ready()},
        :downstream
      )
    end

    {:ok, state}
  end

  defp handle_rtvi_message("send-text", payload, push_fn, state) do
    text = get_in(payload, ["data", "text"]) || ""

    push_fn.(
      %LLMMessagesAppendFrame{
        id: Frame.new_id(),
        messages: [%{"role" => "user", "content" => text}]
      },
      :downstream
    )

    {:ok, state}
  end

  defp handle_rtvi_message("disconnect-bot", _payload, push_fn, state) do
    push_fn.(%EndFrame{id: Frame.new_id()}, :downstream)
    {:ok, state}
  end

  defp handle_rtvi_message("llm-function-call-result", payload, push_fn, state) do
    data = payload["data"] || %{}

    push_fn.(
      %FunctionCallResultFrame{
        id: Frame.new_id(),
        function_name: data["function_name"],
        tool_call_id: data["tool_call_id"],
        result: data["result"]
      },
      :downstream
    )

    {:ok, state}
  end

  defp handle_rtvi_message(_type, _payload, _push_fn, state) do
    {:ok, state}
  end
end
