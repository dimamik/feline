defmodule Feline.Processors.Metrics do
  @moduledoc """
  Calculates Time-To-First-Byte (TTFB) metrics for LLM and TTS.

  Tracks the time between an LLM context request and the first LLM token,
  and between the first LLM token and the first TTS audio chunk. Emits
  `MetricsFrame` and `:telemetry` events for each measurement.
  """
  use Feline.Processor

  alias Feline.Frames.{
    LLMContextFrame,
    LLMRunFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    MetricsFrame
  }

  @impl true
  def init(_opts) do
    {:ok,
     %{
       llm_start: nil,
       tts_start: nil,
       llm_ttfb: nil,
       tts_ttfb: nil
     }}
  end

  @impl true
  def handle_frame(%frame_type{} = frame, direction, _push_fn, state)
      when frame_type in [LLMContextFrame, LLMRunFrame] do
    {:push, frame, direction, %{state | llm_start: System.monotonic_time(:millisecond)}}
  end

  def handle_frame(%LLMTextFrame{} = frame, direction, _push_fn, %{llm_start: start} = state)
      when start != nil do
    now = System.monotonic_time(:millisecond)
    ttfb = now - start

    :telemetry.execute([:feline, :metrics, :llm_ttfb], %{duration: ttfb}, %{})

    metrics_frame = %MetricsFrame{
      id: make_ref(),
      data: %{type: :llm_ttfb, value: ttfb}
    }

    {:push_many, [{metrics_frame, direction}, {frame, direction}],
     %{state | llm_start: nil, llm_ttfb: ttfb, tts_start: now}}
  end

  def handle_frame(%TTSAudioRawFrame{} = frame, direction, _push_fn, %{tts_start: start} = state)
      when start != nil do
    now = System.monotonic_time(:millisecond)
    ttfb = now - start

    :telemetry.execute([:feline, :metrics, :tts_ttfb], %{duration: ttfb}, %{})

    metrics_frame = %MetricsFrame{
      id: make_ref(),
      data: %{type: :tts_ttfb, value: ttfb}
    }

    {:push_many, [{metrics_frame, direction}, {frame, direction}],
     %{state | tts_start: nil, tts_ttfb: ttfb}}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end
end
