defmodule Feline.Observer do
  @moduledoc """
  Behaviour for observing frame processing in a pipeline.

  Implement `on_process_frame/1` and/or `on_push_frame/1` to receive
  telemetry data about frames as they flow through processors.
  """
  @callback on_process_frame(data :: map()) :: :ok
  @callback on_push_frame(data :: map()) :: :ok

  @optional_callbacks on_process_frame: 1, on_push_frame: 1
end
