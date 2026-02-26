defmodule Feline.Processors.FunctionCallHandlerTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.Frames.{FunctionCallInProgressFrame, FunctionCallResultFrame, TextFrame}

  defmodule Collector do
    use Feline.Processor

    @impl true
    def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      send(state.test_pid, {:collected, frame, direction})
      {:ok, state}
    end
  end

  test "executes registered function and pushes result" do
    functions = %{
      "get_weather" => fn args -> "Sunny in #{args["city"]}" end
    }

    {:ok, handler} =
      Feline.Processor.Server.start_link(
        {Feline.Processors.FunctionCallHandler, [functions: functions]}
      )

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(handler, collector)

    frame = %FunctionCallInProgressFrame{
      id: make_ref(),
      function_name: "get_weather",
      tool_call_id: "call_123",
      arguments: %{"city" => "NYC"}
    }

    Processor.queue_frame(handler, frame, :downstream)

    assert_receive {:collected,
                    %FunctionCallResultFrame{
                      function_name: "get_weather",
                      tool_call_id: "call_123",
                      result: "Sunny in NYC"
                    }, :downstream},
                   500
  end

  test "passes through unknown functions" do
    {:ok, handler} =
      Feline.Processor.Server.start_link(
        {Feline.Processors.FunctionCallHandler, [functions: %{}]}
      )

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(handler, collector)

    frame = %FunctionCallInProgressFrame{
      id: make_ref(),
      function_name: "unknown_fn",
      tool_call_id: "call_456",
      arguments: %{}
    }

    Processor.queue_frame(handler, frame, :downstream)

    assert_receive {:collected, %FunctionCallInProgressFrame{function_name: "unknown_fn"},
                    :downstream},
                   500
  end

  test "passes through non-function-call frames" do
    {:ok, handler} =
      Feline.Processor.Server.start_link(
        {Feline.Processors.FunctionCallHandler, [functions: %{}]}
      )

    {:ok, collector} =
      Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    Processor.link(handler, collector)

    frame = %TextFrame{id: make_ref(), text: "hello"}
    Processor.queue_frame(handler, frame, :downstream)

    assert_receive {:collected, %TextFrame{text: "hello"}, :downstream}, 500
  end
end
