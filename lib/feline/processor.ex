defmodule Feline.Processor do
  @moduledoc """
  Behaviour for pipeline processors.

  Each processor implements callbacks to receive frames, transform them,
  and push results downstream or upstream. Use `use Feline.Processor` to
  adopt the behaviour and generate a `child_spec/1` that wraps the module
  in a `Feline.Processor.Server` GenServer.

  ## Callbacks

    * `init/1` — initialize processor state from keyword options
    * `handle_frame/4` — process a frame, return `{:ok, state}`,
      `{:push, frame, direction, state}`, or `{:push_many, frames, state}`
    * `handle_info/3` — handle non-frame messages (optional)
    * `handle_setup/2` — called once after pipeline linking (optional)
    * `handle_cleanup/1` — called on processor shutdown (optional)
  """
  @type direction :: :downstream | :upstream
  @type state :: term()

  @type push_fn :: (struct(), direction() -> :ok)

  @callback init(opts :: keyword()) :: {:ok, state}

  @callback handle_frame(frame :: struct(), direction, push_fn, state) ::
              {:ok, state}
              | {:push, struct(), direction, state}
              | {:push_many, [{struct(), direction}], state}

  @callback handle_info(msg :: term(), push_fn, state) :: {:ok, state}

  @callback handle_setup(setup :: map(), state) :: {:ok, state}
  @callback handle_cleanup(state) :: :ok

  @optional_callbacks handle_info: 3, handle_setup: 2, handle_cleanup: 1

  defmacro __using__(_opts) do
    quote do
      @behaviour Feline.Processor

      def child_spec(opts) do
        %{
          id: {__MODULE__, make_ref()},
          start: {Feline.Processor.Server, :start_link, [{__MODULE__, opts}]},
          restart: :temporary
        }
      end
    end
  end

  def queue_frame(pid, frame, direction \\ :downstream) do
    tag = if Feline.Frame.system?(frame), do: :system_frame, else: :frame
    send(pid, {tag, frame, direction})
    :ok
  end

  def link(processor, next_processor) do
    GenServer.call(processor, {:link_next, next_processor})
    GenServer.call(next_processor, {:link_prev, processor})
    :ok
  end

  def setup(pid, setup) do
    GenServer.call(pid, {:setup, setup})
  end
end
