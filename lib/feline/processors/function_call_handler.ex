defmodule Feline.Processors.FunctionCallHandler do
  @moduledoc """
  Intercepts FunctionCallInProgressFrame, executes the registered function,
  and pushes FunctionCallResultFrame downstream.
  """
  use Feline.Processor

  alias Feline.Frames.{FunctionCallInProgressFrame, FunctionCallResultFrame}

  @impl true
  def init(opts) do
    functions = Keyword.get(opts, :functions, %{})
    {:ok, %{functions: functions}}
  end

  @impl true
  def handle_frame(
        %FunctionCallInProgressFrame{function_name: name} = frame,
        :downstream,
        _push_fn,
        state
      ) do
    case Map.fetch(state.functions, name) do
      {:ok, handler} ->
        result =
          try do
            handler.(frame.arguments)
          rescue
            e -> {:error, Exception.message(e)}
          end

        result_frame = %FunctionCallResultFrame{
          id: make_ref(),
          function_name: name,
          tool_call_id: frame.tool_call_id,
          result: result
        }

        {:push, result_frame, :downstream, state}

      :error ->
        {:push, frame, :downstream, state}
    end
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end
end
