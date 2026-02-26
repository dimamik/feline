defmodule Feline.Services.ElevenLabs.TTS do
  @moduledoc """
  ElevenLabs text-to-speech service. Sends text to ElevenLabs API
  and returns audio as TTSAudioRawFrame.
  """
  use Feline.Services.TTS

  alias Feline.Frames.TTSAudioRawFrame

  @default_model "eleven_turbo_v2_5"
  @default_output_format "pcm_24000"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       voice_id: Keyword.fetch!(opts, :voice_id),
       model_id: Keyword.get(opts, :model_id, @default_model),
       output_format: Keyword.get(opts, :output_format, @default_output_format),
       sample_rate: Keyword.get(opts, :sample_rate, 24_000)
     }}
  end

  @impl Feline.Services.TTS
  def run_tts(text, state) do
    url =
      "https://api.elevenlabs.io/v1/text-to-speech/#{state.voice_id}?" <>
        URI.encode_query(output_format: state.output_format)

    body = %{
      text: text,
      model_id: state.model_id
    }

    case Req.post(url,
           json: body,
           headers: [
             {"xi-api-key", state.api_key},
             {"content-type", "application/json"}
           ]
         ) do
      {:ok, %{status: 200, body: audio}} when is_binary(audio) ->
        frame = %TTSAudioRawFrame{
          id: make_ref(),
          audio: audio,
          sample_rate: state.sample_rate
        }

        {:ok, [frame], state}

      {:ok, %{status: status, body: body}} ->
        raise "ElevenLabs API error (#{status}): #{inspect(body)}"

      {:error, error} ->
        raise "ElevenLabs request failed: #{inspect(error)}"
    end
  end
end
