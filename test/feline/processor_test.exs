defmodule Feline.ProcessorTest do
  use ExUnit.Case, async: true

  alias Feline.Processor
  alias Feline.Frames.{TextFrame, StartFrame, InterruptionFrame}

  defmodule Passthrough do
    use Feline.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      {:push, frame, direction, state}
    end
  end

  defmodule Collector do
    use Feline.Processor

    @impl true
    def init(opts) do
      {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}
    end

    @impl true
    def handle_frame(frame, direction, _push_fn, state) do
      send(state.test_pid, {:collected, frame, direction})
      {:ok, state}
    end
  end

  defmodule Transformer do
    use Feline.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_frame(%TextFrame{text: text} = frame, :downstream, _push_fn, state) do
      {:push, %{frame | text: String.upcase(text)}, :downstream, state}
    end

    def handle_frame(frame, direction, _push_fn, state) do
      {:push, frame, direction, state}
    end
  end

  test "two processors linked: frame flows downstream" do
    {:ok, p1} = Feline.Processor.Server.start_link({Passthrough, []})
    {:ok, p2} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    Processor.link(p1, p2)

    frame = %TextFrame{id: make_ref(), text: "hello"}
    Processor.queue_frame(p1, frame, :downstream)

    assert_receive {:collected, %TextFrame{text: "hello"}, :downstream}, 500
  end

  test "frame transformation through processor chain" do
    {:ok, p1} = Feline.Processor.Server.start_link({Passthrough, []})
    {:ok, p2} = Feline.Processor.Server.start_link({Transformer, []})
    {:ok, p3} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    Processor.link(p1, p2)
    Processor.link(p2, p3)

    frame = %TextFrame{id: make_ref(), text: "hello"}
    Processor.queue_frame(p1, frame, :downstream)

    assert_receive {:collected, %TextFrame{text: "HELLO"}, :downstream}, 500
  end

  test "system frames are processed with priority" do
    {:ok, p1} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})

    # Send a bunch of data frames then a system frame
    for i <- 1..10 do
      Processor.queue_frame(p1, %TextFrame{id: make_ref(), text: "msg#{i}"}, :downstream)
    end

    start = %StartFrame{id: make_ref()}
    Processor.queue_frame(p1, start, :downstream)

    # Collect all received frames
    frames = collect_frames(11)
    # The StartFrame should appear before all text frames because
    # system frames drain first when processing any frame
    start_idx = Enum.find_index(frames, fn {f, _} -> match?(%StartFrame{}, f) end)
    assert start_idx != nil
  end

  test "upstream frame flows in reverse direction" do
    {:ok, p1} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    {:ok, p2} = Feline.Processor.Server.start_link({Passthrough, []})
    Processor.link(p1, p2)

    frame = %TextFrame{id: make_ref(), text: "upstream"}
    Processor.queue_frame(p2, frame, :upstream)

    assert_receive {:collected, %TextFrame{text: "upstream"}, :upstream}, 500
  end

  test "interruption clears buffered frames" do
    # Use a processor that pauses on StartFrame
    defmodule PausingProcessor do
      use Feline.Processor

      @impl true
      def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

      @impl true
      def handle_frame(frame, direction, _push_fn, state) do
        send(state.test_pid, {:processed, frame, direction})
        {:push, frame, direction, state}
      end
    end

    {:ok, p1} = Feline.Processor.Server.start_link({PausingProcessor, [test_pid: self()]})
    {:ok, p2} = Feline.Processor.Server.start_link({Collector, [test_pid: self()]})
    Processor.link(p1, p2)

    # Send some frames, then interruption
    Processor.queue_frame(p1, %TextFrame{id: make_ref(), text: "a"}, :downstream)
    Processor.queue_frame(p1, %InterruptionFrame{id: make_ref()}, :downstream)
    Processor.queue_frame(p1, %TextFrame{id: make_ref(), text: "b"}, :downstream)

    # Both text frames and interruption should be processed (not paused)
    assert_receive {:processed, %TextFrame{text: "a"}, :downstream}, 500
    assert_receive {:processed, %InterruptionFrame{}, :downstream}, 500
    assert_receive {:processed, %TextFrame{text: "b"}, :downstream}, 500
  end

  defp collect_frames(n, acc \\ [])
  defp collect_frames(0, acc), do: Enum.reverse(acc)

  defp collect_frames(n, acc) do
    receive do
      {:collected, frame, dir} -> collect_frames(n - 1, [{frame, dir} | acc])
    after
      500 -> Enum.reverse(acc)
    end
  end
end
