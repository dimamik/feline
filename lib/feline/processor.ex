defmodule Feline.Processor do
  @moduledoc """
  Behaviour for pipeline processors. Each processor is hosted by a
  `Feline.Processor.Server` GenServer.

  Callbacks must return quickly - slow work (HTTP calls, streaming) belongs in
  `{:async, fun, state}`, which runs `fun.(ctx)` in a monitored task. While a
  task runs, incoming data/control frames queue in order; system frames are
  handled immediately. An interruption kills the task and flushes the queue
  (keeping uninterruptible frames).

  `ctx` is a map with:

    * `:push` - `push.(frame, :downstream | :upstream)`, safe to call from tasks
    * `:self` - the hosting server pid (send it messages for `handle_info/3`)
    * `:name` - the processor name
  """

  @type direction :: :downstream | :upstream
  @type ctx :: %{push: (struct(), direction -> :ok), self: pid(), name: term()}
  @type result ::
          {:ok, state :: term()}
          | {:push, struct(), direction, state :: term()}
          | {:push_many, [{struct(), direction}], state :: term()}
          | {:async, (ctx -> term()), state :: term()}

  @callback init(keyword()) :: {:ok, term()}
  @callback handle_setup(start_frame :: struct(), ctx, state :: term()) :: {:ok, term()}
  @callback handle_frame(frame :: struct(), direction, ctx, state :: term()) :: result
  @callback handle_info(message :: term(), ctx, state :: term()) :: result
  @callback handle_cleanup(state :: term()) :: term()

  defmacro __using__(_opts) do
    quote do
      @behaviour Feline.Processor

      @impl true
      def init(opts), do: {:ok, Map.new(opts)}

      @impl true
      def handle_setup(_start_frame, _ctx, state), do: {:ok, state}

      @impl true
      def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

      @impl true
      def handle_info(_message, _ctx, state), do: {:ok, state}

      @impl true
      def handle_cleanup(_state), do: :ok

      defoverridable init: 1,
                     handle_setup: 3,
                     handle_frame: 4,
                     handle_info: 3,
                     handle_cleanup: 1
    end
  end
end
