defmodule Feline.RTVI.Messages do
  @moduledoc """
  Pure functions for building RTVI protocol message maps.

  All messages follow the RTVI v1.2.0 envelope:
  `%{id: short_uuid, label: "rtvi-ai", type: "...", data: %{...}}`
  """

  @label "rtvi-ai"
  @protocol_version "1.2.0"

  def bot_ready do
    msg("bot-ready", %{version: @protocol_version})
  end

  def error(message, fatal \\ false) do
    msg("error", %{message: message, fatal: fatal})
  end

  def user_started_speaking, do: msg("user-started-speaking", %{})
  def user_stopped_speaking, do: msg("user-stopped-speaking", %{})
  def bot_started_speaking, do: msg("bot-started-speaking", %{})
  def bot_stopped_speaking, do: msg("bot-stopped-speaking", %{})

  def user_transcription(text, final) do
    msg("user-transcription", %{text: text, final: final, user_id: "", timestamp: ""})
  end

  def bot_llm_text(text) do
    msg("bot-llm-text", %{text: text})
  end

  def bot_llm_started, do: msg("bot-llm-started", %{})
  def bot_llm_stopped, do: msg("bot-llm-stopped", %{})

  def bot_tts_text(text) do
    msg("bot-tts-text", %{text: text})
  end

  def bot_tts_started, do: msg("bot-tts-started", %{})
  def bot_tts_stopped, do: msg("bot-tts-stopped", %{})

  def bot_output(text, spoken \\ true) do
    msg("bot-output", %{text: text, spoken: spoken})
  end

  def function_call_in_progress(function_name, tool_call_id, arguments) do
    msg("llm-function-call-in-progress", %{
      function_name: function_name,
      tool_call_id: tool_call_id,
      arguments: arguments
    })
  end

  defp msg(type, data) do
    %{
      id: short_id(),
      label: @label,
      type: type,
      data: data
    }
  end

  defp short_id do
    8
    |> :crypto.strong_rand_bytes()
    |> Base.hex_encode32(case: :lower, padding: false)
    |> binary_part(0, 8)
  end
end
