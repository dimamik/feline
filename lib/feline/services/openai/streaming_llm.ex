defmodule Feline.Services.OpenAI.StreamingLLM do
  @moduledoc """
  OpenAI LLM service with streaming SSE support. Spawns a task per request
  that pushes LLMTextFrame tokens as they arrive. Supports interruption
  by killing the in-flight task.
  """
  use Feline.Processor

  alias Feline.Frames.{
    ErrorFrame,
    InterruptionFrame,
    LLMContextFrame,
    LLMTextFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame,
    FunctionCallInProgressFrame
  }

  @default_model "gpt-4.1"
  @default_base_url "https://api.openai.com/v1"

  @impl Feline.Processor
  def init(opts) do
    {:ok, task_sup} = Task.Supervisor.start_link()

    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       model: Keyword.get(opts, :model, @default_model),
       base_url: Keyword.get(opts, :base_url, @default_base_url),
       temperature: Keyword.get(opts, :temperature),
       max_tokens: Keyword.get(opts, :max_tokens),
       task_sup: task_sup,
       task_pid: nil,
       task_ref: nil
     }}
  end

  @impl Feline.Processor
  def handle_frame(%LLMContextFrame{context: ctx}, :downstream, push_fn, state) do
    state = kill_task(state)

    push_fn.(%LLMFullResponseStartFrame{id: make_ref()}, :downstream)

    config = Map.take(state, [:api_key, :model, :base_url, :temperature, :max_tokens])

    %Task{pid: pid, ref: ref} =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        stream_completion(ctx, push_fn, config)
      end)

    {:ok, %{state | task_pid: pid, task_ref: ref}}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    state = kill_task(state)
    push_fn.(frame, direction)
    {:ok, state}
  end

  def handle_frame(frame, direction, push_fn, state) do
    push_fn.(frame, direction)
    {:ok, state}
  end

  @impl Feline.Processor
  def handle_info({ref, _result}, _push_fn, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:ok, %{state | task_pid: nil, task_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, push_fn, %{task_ref: ref} = state)
      when reason != :normal do
    push_fn.(%LLMFullResponseEndFrame{id: make_ref()}, :downstream)
    {:ok, %{state | task_pid: nil, task_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, :normal}, _push_fn, %{task_ref: ref} = state) do
    {:ok, %{state | task_pid: nil, task_ref: nil}}
  end

  def handle_info(_msg, _push_fn, state) do
    {:ok, state}
  end

  defp kill_task(%{task_pid: nil} = state), do: state

  defp kill_task(%{task_pid: pid, task_ref: ref} = state) do
    Task.Supervisor.terminate_child(state.task_sup, pid)
    Process.demonitor(ref, [:flush])
    %{state | task_pid: nil, task_ref: nil}
  end

  defp stream_completion(context, push_fn, config) do
    body =
      %{model: config.model, messages: Feline.Context.messages(context), stream: true}
      |> maybe_put(:temperature, config.temperature)
      |> maybe_put(:max_tokens, config.max_tokens)
      |> maybe_put_tools(context.tools)

    case Req.post(
           "#{config.base_url}/chat/completions",
           json: body,
           headers: [
             {"authorization", "Bearer #{config.api_key}"},
             {"content-type", "application/json"}
           ],
           into: :self
         ) do
      {:ok, %{status: 200}} ->
        tool_calls = receive_sse_chunks(push_fn, %{})
        flush_tool_calls(tool_calls, push_fn)

      {:ok, %{status: status, body: body}} ->
        raise "OpenAI API error (#{status}): #{inspect(body)}"

      {:error, error} ->
        raise "OpenAI API request failed: #{inspect(error)}"
    end

    push_fn.(%LLMFullResponseEndFrame{id: make_ref()}, :downstream)
  end

  defp receive_sse_chunks(push_fn, tool_calls_acc) do
    receive do
      {_ref, {:data, data}} ->
        tool_calls_acc = process_sse_data(data, push_fn, tool_calls_acc)
        receive_sse_chunks(push_fn, tool_calls_acc)

      {_ref, :done} ->
        tool_calls_acc
    after
      30_000 ->
        push_fn.(
          %ErrorFrame{id: make_ref(), message: "SSE stream timed out after 30s"},
          :upstream
        )

        tool_calls_acc
    end
  end

  defp process_sse_data(data, push_fn, acc) do
    data
    |> String.split("\n")
    |> Enum.reduce(acc, fn line, acc ->
      line |> String.trim() |> process_sse_line(push_fn, acc)
    end)
  end

  defp process_sse_line("data: [DONE]", _push_fn, acc), do: acc

  defp process_sse_line("data: " <> json_str, push_fn, acc) do
    case Jason.decode(json_str) do
      {:ok, %{"choices" => [%{"delta" => delta} | _]}} ->
        process_delta(delta, push_fn, acc)

      _ ->
        acc
    end
  end

  defp process_sse_line(_line, _push_fn, acc), do: acc

  defp process_delta(%{"content" => content}, push_fn, acc)
       when is_binary(content) and content != "" do
    push_fn.(%LLMTextFrame{id: make_ref(), text: content}, :downstream)
    acc
  end

  defp process_delta(%{"tool_calls" => tool_calls}, _push_fn, acc) do
    Enum.reduce(tool_calls, acc, fn tc, acc ->
      idx = tc["index"] || 0
      existing = Map.get(acc, idx, %{id: nil, name: "", arguments: ""})
      fn_chunk = tc["function"] || %{}

      updated = %{
        id: tc["id"] || existing.id,
        name: if(tc["id"], do: fn_chunk["name"] || "", else: existing.name),
        arguments: existing.arguments <> (fn_chunk["arguments"] || "")
      }

      Map.put(acc, idx, updated)
    end)
  end

  defp process_delta(_delta, _push_fn, acc), do: acc

  defp flush_tool_calls(tool_calls, push_fn) do
    for {_idx, tc} <- Enum.sort(tool_calls) do
      push_fn.(
        %FunctionCallInProgressFrame{
          id: make_ref(),
          function_name: tc.name,
          tool_call_id: tc.id,
          arguments: parse_arguments(tc.arguments)
        },
        :downstream
      )
    end
  end

  defp parse_arguments(""), do: %{}

  defp parse_arguments(json_str) do
    case Jason.decode(json_str) do
      {:ok, args} -> args
      _ -> %{"raw" => json_str}
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_tools(map, []), do: map
  defp maybe_put_tools(map, tools), do: Map.put(map, :tools, tools)
end
