defmodule Feline.RTVI.Reporter do
  @moduledoc """
  Translates pipeline frames into RTVI event messages for the client
  (`user-transcription`, `bot-llm-text`, speaking events, `bot-interrupted`,
  ...). Place immediately before the transport output so it sees the
  downstream flow plus everything the output transport pushes upstream
  (bot-speaking events).

  Every frame is forwarded untouched; events are emitted as additional
  `OutputTransportMessageFrame`s.
  """

  use Feline.Processor

  alias Feline.Frames.All.{
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InterimTranscriptionFrame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    OutputTransportMessageFrame,
    TranscriptionFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSTextFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame
  }

  @label "rtvi-ai"

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_frame(frame, direction, _ctx, state) do
    case event_for(frame) do
      nil ->
        {:push, frame, direction, state}

      event ->
        {:push_many,
         [{frame, direction}, {%OutputTransportMessageFrame{message: event}, :downstream}], state}
    end
  end

  defp event_for(%UserStartedSpeakingFrame{}), do: event("user-started-speaking")
  defp event_for(%UserStoppedSpeakingFrame{}), do: event("user-stopped-speaking")
  defp event_for(%BotStartedSpeakingFrame{}), do: event("bot-started-speaking")
  defp event_for(%BotStoppedSpeakingFrame{}), do: event("bot-stopped-speaking")
  defp event_for(%InterruptionFrame{}), do: event("bot-interrupted")
  defp event_for(%LLMFullResponseStartFrame{}), do: event("bot-llm-started")
  defp event_for(%LLMFullResponseEndFrame{}), do: event("bot-llm-stopped")
  defp event_for(%TTSStartedFrame{}), do: event("bot-tts-started")
  defp event_for(%TTSStoppedFrame{}), do: event("bot-tts-stopped")

  defp event_for(%LLMTextFrame{text: text}), do: event("bot-llm-text", %{"text" => text})
  defp event_for(%TTSTextFrame{text: text}), do: event("bot-tts-text", %{"text" => text})

  defp event_for(%TranscriptionFrame{} = frame) do
    event("user-transcription", %{
      "text" => frame.text,
      "user_id" => frame.user_id,
      "timestamp" => frame.timestamp,
      "final" => true
    })
  end

  defp event_for(%InterimTranscriptionFrame{} = frame) do
    event("user-transcription", %{
      "text" => frame.text,
      "user_id" => frame.user_id,
      "timestamp" => frame.timestamp,
      "final" => false
    })
  end

  defp event_for(_frame), do: nil

  defp event(type, data \\ nil) do
    message = %{"label" => @label, "type" => type}
    if data, do: Map.put(message, "data", data), else: message
  end
end
