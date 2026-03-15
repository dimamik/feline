defmodule Feline.RTVI.EventEmitter do
  @moduledoc """
  Processor that converts pipeline frames to RTVI JSON messages.

  Place late in the pipeline (just before `WebSocket.Output`). For each
  recognized frame, pushes an `OutputTransportMessageFrame` containing
  the RTVI envelope, then passes the original frame through.

  Note: `BotStartedSpeakingFrame` / `BotStoppedSpeakingFrame` are
  emitted by `WebSocket.Output` (after this processor), so those RTVI
  messages are handled by Output's `rtvi_enabled` option instead.
  """
  use Feline.Processor

  alias Feline.Frame
  alias Feline.RTVI.Messages

  alias Feline.Frames.{
    FunctionCallInProgressFrame,
    InterimTranscriptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    OutputTransportMessageFrame,
    TextFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame
  }

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_frame(%UserStartedSpeakingFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.user_started_speaking())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%UserStoppedSpeakingFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.user_stopped_speaking())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%TranscriptionFrame{text: text} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.user_transcription(text, true))
    {:push, frame, :downstream, state}
  end

  def handle_frame(%InterimTranscriptionFrame{text: text} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.user_transcription(text, false))
    {:push, frame, :downstream, state}
  end

  def handle_frame(%LLMTextFrame{text: text} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_llm_text(text))
    {:push, frame, :downstream, state}
  end

  def handle_frame(%LLMFullResponseStartFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_llm_started())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%LLMFullResponseEndFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_llm_stopped())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%TextFrame{text: text} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_tts_text(text))
    {:push, frame, :downstream, state}
  end

  def handle_frame(%TTSStartedFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_tts_started())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%TTSStoppedFrame{} = frame, :downstream, push_fn, state) do
    emit(push_fn, Messages.bot_tts_stopped())
    {:push, frame, :downstream, state}
  end

  def handle_frame(%FunctionCallInProgressFrame{} = frame, :downstream, push_fn, state) do
    emit(
      push_fn,
      Messages.function_call_in_progress(frame.function_name, frame.tool_call_id, frame.arguments)
    )

    {:push, frame, :downstream, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  defp emit(push_fn, payload) do
    push_fn.(%OutputTransportMessageFrame{id: Frame.new_id(), payload: payload}, :downstream)
  end
end
