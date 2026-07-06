defmodule Feline.RTVI.Processor do
  @moduledoc """
  Handles inbound RTVI protocol messages from pipecat clients
  (`@pipecat-ai/client-js` and friends). Place near the top of the pipeline,
  right after the transport input.

  Handles: `client-ready` (responds `bot-ready`), `send-text` (interrupts and
  appends a user message), `llm-function-call-result`, `disconnect-bot`.
  Unknown types get an `error-response`.
  """

  use Feline.Processor

  alias Feline.Frames.All.{
    EndFrame,
    ErrorFrame,
    FunctionCallResultFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    LLMMessagesAppendFrame,
    OutputTransportMessageUrgentFrame
  }

  require Logger

  @protocol_version "2.0.0"
  @label "rtvi-ai"

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_frame(%InputTransportMessageFrame{message: message}, :downstream, _ctx, state) do
    case message do
      %{"label" => @label, "type" => type} -> handle_message(type, message, state)
      _other -> {:ok, state}
    end
  end

  def handle_frame(%ErrorFrame{} = frame, :upstream, _ctx, state) do
    error_message = %{
      "label" => @label,
      "type" => "error",
      "data" => %{"error" => to_string(frame.error), "fatal" => frame.fatal}
    }

    {:push_many,
     [{frame, :upstream}, {%OutputTransportMessageUrgentFrame{message: error_message}, :downstream}],
     state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp handle_message("client-ready", message, state) do
    Logger.info("RTVI client ready: #{inspect(message["data"])}")

    bot_ready = %{
      "label" => @label,
      "type" => "bot-ready",
      "id" => message["id"] || "",
      "data" => %{
        "version" => @protocol_version,
        "about" => %{"library" => "feline", "library_version" => "0.1.0"}
      }
    }

    {:push, %OutputTransportMessageUrgentFrame{message: bot_ready}, :downstream, state}
  end

  defp handle_message("send-text", %{"data" => %{"content" => content}}, state) do
    append = %LLMMessagesAppendFrame{
      messages: [%{"role" => "user", "content" => content}],
      run_llm: true
    }

    {:push_many,
     [
       {%InterruptionFrame{}, :downstream},
       {%InterruptionFrame{}, :upstream},
       {append, :downstream}
     ], state}
  end

  defp handle_message("llm-function-call-result", %{"data" => data}, state) do
    result = %FunctionCallResultFrame{
      function_name: data["function_name"],
      tool_call_id: data["tool_call_id"],
      arguments: data["arguments"] || %{},
      result: data["result"]
    }

    {:push, result, :downstream, state}
  end

  defp handle_message("disconnect-bot", _message, state) do
    {:push, %EndFrame{}, :downstream, state}
  end

  defp handle_message(type, message, state) do
    error_response = %{
      "label" => @label,
      "type" => "error-response",
      "id" => message["id"] || "",
      "data" => %{"error" => "Unsupported type #{type}"}
    }

    {:push, %OutputTransportMessageUrgentFrame{message: error_response}, :downstream, state}
  end
end
