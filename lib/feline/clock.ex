defmodule Feline.Clock do
  @moduledoc """
  Monotonic clock wrapper for time measurements.
  """
  def monotonic_time_ns, do: System.monotonic_time(:nanosecond)
end
