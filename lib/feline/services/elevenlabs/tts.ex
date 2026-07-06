defmodule Feline.Services.ElevenLabs.TTS do
  @moduledoc """
  Text-to-speech via ElevenLabs HTTP API.

  Each `TextFrame` triggers one synthesis request and emits:

    * `TTSStartedFrame`
    * `TTSTextFrame`
    * one `TTSAudioRawFrame` with PCM audio
    * `TTSStoppedFrame`

  Requests run in an async task, so pipeline interruptions can cancel in-flight
  synthesis.

  Options:
    * `:api_key` (defaults to `ELEVENLABS_API_KEY`)
    * `:voice_id` (defaults to `ELEVENLABS_VOICE_ID` or a built-in voice)
    * `:model` (default `eleven_multilingual_v2`)
    * `:base_url` (default `https://api.elevenlabs.io/v1`)
    * `:output_format` (default `pcm_24000`)
    * `:sample_rate` (default `24_000`)
    * `:req_options` (forwarded to `Req.post/1`)
  """

  use Feline.Processor

  alias Feline.Frames.All.{
    TextFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSTextFrame
  }

  require Logger

  @default_voice_id "JBFqnCBsd6RMkjVDRZzb"

  @impl true
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.get(opts, :api_key) || System.get_env("ELEVENLABS_API_KEY"),
       voice_id:
         Keyword.get(opts, :voice_id) || System.get_env("ELEVENLABS_VOICE_ID") ||
           @default_voice_id,
       model: Keyword.get(opts, :model, "eleven_multilingual_v2"),
       base_url: Keyword.get(opts, :base_url, "https://api.elevenlabs.io/v1"),
       output_format: Keyword.get(opts, :output_format, "pcm_24000"),
       sample_rate: Keyword.get(opts, :sample_rate, 24_000),
       req_options: Keyword.get(opts, :req_options, [])
     }}
  end

  @impl true
  def handle_frame(%TextFrame{text: text}, :downstream, _ctx, state) do
    if String.trim(text) == "" do
      {:ok, state}
    else
      {:async, fn ctx -> synthesize(text, ctx, state) end, state}
    end
  end

  def handle_frame(frame, direction, _ctx, state), do: {:push, frame, direction, state}

  defp synthesize(text, ctx, state) do
    ctx.push.(%TTSStartedFrame{}, :downstream)
    ctx.push.(%TTSTextFrame{text: text}, :downstream)

    body = %{
      text: text,
      model_id: state.model
    }

    url =
      state.base_url <>
        "/text-to-speech/#{state.voice_id}?" <>
        URI.encode_query(output_format: state.output_format)

    case Req.post(
           [
             url: url,
             json: body,
             headers: [{"xi-api-key", state.api_key || ""}, {"accept", "audio/pcm"}],
             receive_timeout: 60_000
           ] ++ state.req_options
         ) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 ->
        case audio_binary(body) do
          nil ->
            Logger.warning("ElevenLabs TTS returned non-binary audio payload")

          audio ->
            ctx.push.(
              %TTSAudioRawFrame{
                audio: audio,
                sample_rate: state.sample_rate,
                num_channels: 1
              },
              :downstream
            )
        end

      {:ok, %Req.Response{status: status}} ->
        Logger.warning("ElevenLabs TTS failed: status=#{status}")

      {:error, reason} ->
        Logger.warning("ElevenLabs TTS request failed: #{inspect(reason)}")
    end

    ctx.push.(%TTSStoppedFrame{}, :downstream)
  end

  defp audio_binary(binary) when is_binary(binary), do: binary
  defp audio_binary(iodata) when is_list(iodata), do: IO.iodata_to_binary(iodata)
  defp audio_binary(_other), do: nil
end
