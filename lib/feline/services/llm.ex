defmodule Feline.Services.LLM do
  @moduledoc """
  Behaviour for LLM services. Processes LLM context frames and produces
  text output frames (LLMTextFrame, LLMFullResponseStartFrame, etc.).
  """

  alias Feline.Frames.{
    LLMContextFrame,
    LLMRunFrame,
    LLMFullResponseStartFrame,
    LLMFullResponseEndFrame
  }

  @callback process_context(context :: term(), state :: term()) ::
              {:ok, output_frames :: [struct()], state :: term()}

  defmacro __using__(_opts) do
    quote do
      use Feline.Processor

      @behaviour Feline.Services.LLM

      @impl Feline.Processor
      def handle_frame(%LLMContextFrame{context: ctx}, :downstream, _push_fn, state) do
        {:ok, frames, state} = process_context(ctx, state)

        out =
          [{%LLMFullResponseStartFrame{id: make_ref()}, :downstream}] ++
            Enum.map(frames, &{&1, :downstream}) ++
            [{%LLMFullResponseEndFrame{id: make_ref()}, :downstream}]

        {:push_many, out, state}
      end

      def handle_frame(%LLMRunFrame{}, :downstream, _push_fn, state) do
        {:ok, state}
      end

      def handle_frame(frame, direction, _push_fn, state) do
        {:push, frame, direction, state}
      end

      defoverridable handle_frame: 4
    end
  end
end
