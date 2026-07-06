defmodule Feline.Frames do
  @moduledoc """
  Frame definitions. Frames are plain structs flowing through the pipeline,
  downstream (input -> output) or upstream.

  Categories mirror pipecat:

    * `:system` - processed immediately, never queued behind pending work and
      never flushed by interruptions.
    * `:data` / `:control` - processed in order; flushed by interruptions
      unless the frame is uninterruptible.
  """

  defmacro defframe(name, category, opts \\ []) do
    fields = Keyword.get(opts, :fields, [])
    uninterruptible = Keyword.get(opts, :uninterruptible, false)

    quote do
      defmodule unquote(name) do
        defstruct unquote(fields)

        @doc false
        def __frame__(:category), do: unquote(category)
        def __frame__(:uninterruptible), do: unquote(uninterruptible)
      end
    end
  end
end

defmodule Feline.Frame do
  @moduledoc "Helpers over frame structs."

  def category(%module{}), do: module.__frame__(:category)
  def system?(frame), do: category(frame) == :system
  def uninterruptible?(%module{}), do: module.__frame__(:uninterruptible)
end

defmodule Feline.Frames.All do
  @moduledoc false
  import Feline.Frames

  # -- System frames --

  defframe StartFrame, :system,
    fields: [audio_in_sample_rate: 16_000, audio_out_sample_rate: 24_000]

  defframe CancelFrame, :system
  defframe ErrorFrame, :system, fields: [error: nil, fatal: false]
  defframe InterruptionFrame, :system

  defframe UserStartedSpeakingFrame, :system
  defframe UserStoppedSpeakingFrame, :system
  defframe VADUserStartedSpeakingFrame, :system
  defframe VADUserStoppedSpeakingFrame, :system
  defframe BotStartedSpeakingFrame, :system
  defframe BotStoppedSpeakingFrame, :system

  defframe InputAudioRawFrame, :system, fields: [audio: <<>>, sample_rate: 16_000, num_channels: 1]
  defframe InputTransportMessageFrame, :system, fields: [message: %{}]
  defframe OutputTransportMessageUrgentFrame, :system, fields: [message: %{}]

  # -- Data frames --

  defframe TextFrame, :data, fields: [text: ""]
  defframe LLMTextFrame, :data, fields: [text: ""]
  defframe TTSTextFrame, :data, fields: [text: ""]
  defframe TranscriptionFrame, :data, fields: [text: "", user_id: "", timestamp: ""]
  defframe InterimTranscriptionFrame, :data, fields: [text: "", user_id: "", timestamp: ""]

  defframe OutputAudioRawFrame, :data, fields: [audio: <<>>, sample_rate: 24_000, num_channels: 1]
  defframe TTSAudioRawFrame, :data, fields: [audio: <<>>, sample_rate: 24_000, num_channels: 1]

  defframe LLMContextFrame, :data, fields: [context: nil]
  defframe LLMMessagesAppendFrame, :data, fields: [messages: [], run_llm: false]
  defframe OutputTransportMessageFrame, :data, fields: [message: %{}]

  defframe FunctionCallResultFrame, :data,
    fields: [function_name: nil, tool_call_id: nil, arguments: %{}, result: nil],
    uninterruptible: true

  # -- Control frames --

  defframe EndFrame, :control, uninterruptible: true
  defframe LLMFullResponseStartFrame, :control
  defframe LLMFullResponseEndFrame, :control
  defframe TTSStartedFrame, :control
  defframe TTSStoppedFrame, :control
end
