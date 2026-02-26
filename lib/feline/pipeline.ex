defmodule Feline.Pipeline do
  @moduledoc """
  Struct holding a list of processor specifications for a pipeline.

  Build with `Feline.Pipeline.new/1` and run with `Feline.Pipeline.Task`.
  """
  defstruct [:processor_specs]

  def new(processor_specs) when is_list(processor_specs) do
    %__MODULE__{processor_specs: processor_specs}
  end
end
