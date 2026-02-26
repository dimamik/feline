defmodule Feline.Transport do
  @moduledoc """
  Behaviour for transports. A transport provides input and output
  processor specs that can be included in a pipeline.
  """

  @callback input_spec(opts :: keyword()) :: {module(), keyword()}
  @callback output_spec(opts :: keyword()) :: {module(), keyword()}
end
