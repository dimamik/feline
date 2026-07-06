defmodule Feline.Services.OpenAI.LLM do
  @moduledoc """
  Streaming chat-completions LLM service (OpenAI or any compatible API).

  On `LLMContextFrame` it runs the completion in an async task, pushing
  `LLMFullResponseStartFrame`, one `LLMTextFrame` per content delta, then
  `LLMFullResponseEndFrame`. Because the work is `{:async, ...}`, an
  interruption kills the stream mid-response.

  Tool calls: pass `tools: [%{name: ..., description: ..., parameters: ...,
  handler: fun}]`. Handlers run inside the task (exceptions become error
  results, they don't crash the pipeline) and results are pushed upstream as
  `FunctionCallResultFrame`s for the context aggregator to commit and re-run.

  Options: `:api_key` (defaults to `OPENAI_API_KEY`), `:model` (default
  `gpt-4o-mini`), `:base_url`, `:tools`, `:req_options`.
  """

  use Feline.Processor

  alias Feline.Frames.All.{
    FunctionCallResultFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame
  }

  require Logger

  @impl true
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.get(opts, :api_key) || System.get_env("OPENAI_API_KEY"),
       model: Keyword.get(opts, :model, "gpt-4o-mini"),
       base_url: Keyword.get(opts, :base_url, "https://api.openai.com/v1"),
       tools: Keyword.get(opts, :tools, []),
       req_options: Keyword.get(opts, :req_options, [])
     }}
  end

  @impl true
  def handle_frame(%LLMContextFrame{context: context}, :downstream, _ctx, state) do
    service = state

    {:async,
     fn ctx ->
       ctx.push.(%LLMFullResponseStartFrame{}, :downstream)
       tool_calls = stream_completion(service, context, ctx)
       run_tool_calls(service, tool_calls, ctx)
       ctx.push.(%LLMFullResponseEndFrame{}, :downstream)
     end, state}
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  # -- Completion streaming --

  defp stream_completion(service, context, ctx) do
    body =
      %{
        model: service.model,
        messages: context.messages,
        stream: true
      }
      |> maybe_put_tools(service.tools)

    response =
      Req.post!(
        [
          url: service.base_url <> "/chat/completions",
          json: body,
          auth: {:bearer, service.api_key || ""},
          into: :self,
          receive_timeout: 60_000
        ] ++ service.req_options
      )

    consume_sse(response, ctx, "", %{})
  end

  defp consume_sse(response, ctx, buffer, tool_calls) do
    receive do
      message ->
        case Req.parse_message(response, message) do
          {:ok, parts} ->
            {buffer, tool_calls, done?} =
              Enum.reduce(parts, {buffer, tool_calls, false}, fn
                {:data, data}, {buffer, tool_calls, done?} ->
                  {buffer, tool_calls} = handle_sse_data(buffer <> data, ctx, tool_calls)
                  {buffer, tool_calls, done?}

                :done, {buffer, tool_calls, _done?} ->
                  {buffer, tool_calls, true}

                _other, acc ->
                  acc
              end)

            if done?, do: tool_calls, else: consume_sse(response, ctx, buffer, tool_calls)

          :unknown ->
            consume_sse(response, ctx, buffer, tool_calls)
        end
    end
  end

  defp handle_sse_data(buffer, ctx, tool_calls) do
    {events, rest} = split_sse_events(buffer)

    tool_calls =
      Enum.reduce(events, tool_calls, fn event, tool_calls ->
        case event do
          "[DONE]" -> tool_calls
          json -> handle_chunk(Jason.decode!(json), ctx, tool_calls)
        end
      end)

    {rest, tool_calls}
  end

  defp split_sse_events(buffer) do
    parts = String.split(buffer, "\n\n")
    {events, [rest]} = Enum.split(parts, -1)

    events =
      for event <- events,
          line <- String.split(event, "\n"),
          String.starts_with?(line, "data:"),
          do: line |> String.trim_leading("data:") |> String.trim()

    {events, rest}
  end

  defp handle_chunk(%{"choices" => [%{"delta" => delta} | _rest]}, ctx, tool_calls) do
    if content = delta["content"] do
      if content != "", do: ctx.push.(%LLMTextFrame{text: content}, :downstream)
    end

    Enum.reduce(delta["tool_calls"] || [], tool_calls, &accumulate_tool_call/2)
  end

  defp handle_chunk(_chunk, _ctx, tool_calls), do: tool_calls

  defp accumulate_tool_call(delta, tool_calls) do
    index = delta["index"] || 0
    # Intermediate deltas may omit "function" or its keys entirely.
    function_chunk = delta["function"] || %{}

    Map.update(
      tool_calls,
      index,
      %{
        id: delta["id"],
        name: function_chunk["name"] || "",
        arguments: function_chunk["arguments"] || ""
      },
      fn call ->
        %{
          call
          | id: call.id || delta["id"],
            name: call.name <> (function_chunk["name"] || ""),
            arguments: call.arguments <> (function_chunk["arguments"] || "")
        }
      end
    )
  end

  # -- Tool execution --

  defp run_tool_calls(_service, tool_calls, _ctx) when map_size(tool_calls) == 0, do: :ok

  defp run_tool_calls(service, tool_calls, ctx) do
    for {_index, call} <- Enum.sort_by(tool_calls, fn {index, _call} -> index end) do
      arguments =
        case Jason.decode(call.arguments) do
          {:ok, decoded} -> decoded
          {:error, _reason} -> %{}
        end

      result =
        case Enum.find(service.tools, &(&1.name == call.name)) do
          nil ->
            %{"error" => "unknown function #{call.name}"}

          tool ->
            try do
              tool.handler.(arguments)
            rescue
              exception ->
                Logger.warning("Tool #{call.name} raised: #{Exception.message(exception)}")
                %{"error" => Exception.message(exception)}
            end
        end

      ctx.push.(
        %FunctionCallResultFrame{
          function_name: call.name,
          tool_call_id: call.id,
          arguments: arguments,
          result: result
        },
        :upstream
      )
    end
  end

  defp maybe_put_tools(body, []), do: body

  defp maybe_put_tools(body, tools) do
    specs =
      for tool <- tools do
        %{
          type: "function",
          function: %{
            name: tool.name,
            description: tool[:description] || "",
            parameters: tool[:parameters] || %{type: "object", properties: %{}}
          }
        }
      end

    Map.put(body, :tools, specs)
  end
end
