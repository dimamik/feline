defmodule Feline.TestProcessors do
  @moduledoc "Processors used across tests."

  defmodule Recorder do
    @moduledoc "Records every frame it sees to a listener pid, then forwards."
    use Feline.Processor

    @impl true
    def handle_frame(frame, direction, _ctx, %{listener: listener} = state) do
      send(listener, {:recorded, state[:tag] || :recorder, frame, direction})
      {:push, frame, direction, state}
    end
  end

  defmodule SlowWorker do
    @moduledoc """
    Simulates a slow service: on TextFrame, runs an async task that waits for
    `:finish` (sent by the test to the task via the listener) before pushing
    the text downstream as a new TextFrame with "!done" appended.
    """
    use Feline.Processor

    alias Feline.Frames.All.TextFrame

    @impl true
    def handle_frame(%TextFrame{} = frame, :downstream, _ctx, %{listener: listener} = state) do
      fun = fn ctx ->
        send(listener, {:working_on, frame.text, self()})

        receive do
          :finish -> ctx.push.(%TextFrame{text: frame.text <> "!done"}, :downstream)
        end
      end

      {:async, fun, state}
    end

    def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
  end

  defmodule Crasher do
    @moduledoc "Crashes when it sees a TextFrame with text \"boom\"."
    use Feline.Processor

    alias Feline.Frames.All.TextFrame

    @impl true
    def handle_frame(%TextFrame{text: "boom"}, _direction, _ctx, _state), do: raise("boom")
    def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
  end

  defmodule UpstreamReflector do
    @moduledoc "Bounces frames of the configured module back upstream."
    use Feline.Processor

    @impl true
    def handle_frame(%module{} = frame, :downstream, _ctx, %{reflect: module} = state) do
      {:push, frame, :upstream, state}
    end

    def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}
  end

  defmodule CleanupReporter do
    @moduledoc "Reports handle_cleanup to the listener."
    use Feline.Processor

    @impl true
    def handle_cleanup(%{listener: listener}), do: send(listener, :cleaned_up)
  end
end
