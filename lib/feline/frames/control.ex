defmodule Feline.Frames.EndFrame do
  @moduledoc false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.StopFrame do
  @moduledoc false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.HeartbeatFrame do
  @moduledoc false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.LLMFullResponseStartFrame do
  @moduledoc false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.LLMFullResponseEndFrame do
  @moduledoc false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.TTSStartedFrame do
  @moduledoc false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.TTSStoppedFrame do
  @moduledoc false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.InterruptionCompletionFrame do
  @moduledoc false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, :ref, metadata: %{}]
end
