defmodule Feline.Frames.StartFrame do
  @moduledoc false
  use Feline.Frame, category: :system

  defstruct [
    :id,
    :pts,
    metadata: %{},
    audio_in_sample_rate: 16_000,
    audio_out_sample_rate: 24_000,
    enable_metrics: false,
    enable_usage_metrics: false
  ]
end

defmodule Feline.Frames.CancelFrame do
  @moduledoc false
  use Feline.Frame, category: :system
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.ErrorFrame do
  @moduledoc false
  use Feline.Frame, category: :system

  defstruct [
    :id,
    :pts,
    :message,
    :exception,
    :processor,
    metadata: %{},
    fatal: false
  ]
end

defmodule Feline.Frames.InterruptionFrame do
  @moduledoc false
  use Feline.Frame, category: :system
  defstruct [:id, :pts, :ref, :caller, metadata: %{}]
end

defmodule Feline.Frames.MetricsFrame do
  @moduledoc false
  use Feline.Frame, category: :system
  defstruct [:id, :pts, :data, metadata: %{}]
end
