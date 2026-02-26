defmodule Feline.Frames.TextFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :text, metadata: %{}]
end

defmodule Feline.Frames.LLMTextFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :text, metadata: %{}]
end

defmodule Feline.Frames.OutputAudioRawFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :audio,
    :sample_rate,
    metadata: %{},
    num_channels: 1
  ]
end

defmodule Feline.Frames.TTSAudioRawFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :audio,
    :sample_rate,
    :context_id,
    metadata: %{},
    num_channels: 1
  ]
end

defmodule Feline.Frames.TranscriptionFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :text,
    :language,
    metadata: %{}
  ]
end

defmodule Feline.Frames.InterimTranscriptionFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :text,
    :language,
    metadata: %{}
  ]
end

defmodule Feline.Frames.LLMRunFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.LLMContextFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :context, metadata: %{}]
end

defmodule Feline.Frames.LLMMessagesAppendFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :messages, metadata: %{}]
end

defmodule Feline.Frames.FunctionCallInProgressFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :function_name,
    :tool_call_id,
    :arguments,
    metadata: %{}
  ]
end

defmodule Feline.Frames.FunctionCallResultFrame do
  false
  use Feline.Frame, category: :data, uninterruptible: true

  defstruct [
    :id,
    :pts,
    :function_name,
    :tool_call_id,
    :result,
    metadata: %{}
  ]
end

defmodule Feline.Frames.TTSSpeakFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :text, metadata: %{}]
end

defmodule Feline.Frames.OutputImageRawFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :image, :width, :height, metadata: %{}, format: "RGB"]
end

defmodule Feline.Frames.SpriteFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :images, metadata: %{}]
end

defmodule Feline.Frames.VisionTextFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :text, :image, :width, :height, metadata: %{}, format: "RGB"]
end

defmodule Feline.Frames.InputAudioRawFrame do
  false
  use Feline.Frame, category: :data

  defstruct [
    :id,
    :pts,
    :audio,
    :sample_rate,
    metadata: %{},
    num_channels: 1
  ]
end

defmodule Feline.Frames.UserStartedSpeakingFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.UserStoppedSpeakingFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.BotStartedSpeakingFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.BotStoppedSpeakingFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, metadata: %{}]
end

defmodule Feline.Frames.InputImageRawFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :image, :width, :height, metadata: %{}, format: "RGB"]
end

defmodule Feline.Frames.UserImageRawFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :image, :width, :height, :user_id, metadata: %{}, format: "RGB"]
end

defmodule Feline.Frames.InputTransportMessageFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :payload, metadata: %{}]
end

defmodule Feline.Frames.OutputTransportMessageFrame do
  false
  use Feline.Frame, category: :data
  defstruct [:id, :pts, :payload, metadata: %{}]
end
