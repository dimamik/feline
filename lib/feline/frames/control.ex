defmodule Feline.Frames.EndFrame do
  false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.StopFrame do
  false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.HeartbeatFrame do
  false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.LLMFullResponseStartFrame do
  false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.LLMFullResponseEndFrame do
  false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.TTSStartedFrame do
  false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.TTSStoppedFrame do
  false
  use Feline.Frame, category: :control
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.InterruptionCompletionFrame do
  false
  use Feline.Frame, category: :control, uninterruptible: true
  defstruct [:id, :pts, :ref, metadata: %{}]
end
