defmodule Feline.Frame do
  @moduledoc """
  Frame classification and introspection.

  Provides the `use Feline.Frame` macro for defining frame structs with
  a category (`:system`, `:data`, `:control`) and an optional
  `:uninterruptible` flag. System frames are prioritized in the processor
  mailbox via selective receive.
  """
  @type direction :: :downstream | :upstream
  @type category :: :system | :data | :control

  defmacro __using__(opts) do
    category = Keyword.fetch!(opts, :category)
    uninterruptible = Keyword.get(opts, :uninterruptible, false)

    quote do
      def __frame_category__, do: unquote(category)
      def __frame_uninterruptible__, do: unquote(uninterruptible)
    end
  end

  def system?(%{__struct__: mod}), do: mod.__frame_category__() == :system

  def uninterruptible?(%{__struct__: mod}) do
    Code.ensure_loaded?(mod) and
      function_exported?(mod, :__frame_uninterruptible__, 0) and
      mod.__frame_uninterruptible__()
  end

  def new_id, do: make_ref()
end
