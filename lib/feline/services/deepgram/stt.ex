defmodule Feline.Services.Deepgram.STT do
  @moduledoc """
  Deepgram speech-to-text service. Streams audio to Deepgram's WebSocket
  API and produces TranscriptionFrame results.

  This is a simplified implementation that buffers audio and sends it
  in chunks via REST for initial validation. A full implementation would
  use Deepgram's streaming WebSocket API.
  """
  use Feline.Services.STT

  alias Feline.Frames.TranscriptionFrame

  @default_model "nova-2"
  @default_language "en"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       model: Keyword.get(opts, :model, @default_model),
       language: Keyword.get(opts, :language, @default_language),
       buffer: <<>>,
       buffer_size: Keyword.get(opts, :buffer_size, 32_000),
       sample_rate: Keyword.get(opts, :sample_rate, 16_000)
     }}
  end

  @impl Feline.Services.STT
  def run_stt(audio, state) do
    buffer = state.buffer <> audio

    if byte_size(buffer) >= state.buffer_size do
      case transcribe(buffer, state) do
        {:ok, text} ->
          frame = %TranscriptionFrame{
            id: make_ref(),
            text: text,
            language: state.language
          }

          {:ok, [frame], %{state | buffer: <<>>}}

        {:error, _reason} ->
          {:continue, %{state | buffer: <<>>}}
      end
    else
      {:continue, %{state | buffer: buffer}}
    end
  end

  defp transcribe(audio, state) do
    url =
      "https://api.deepgram.com/v1/listen?" <>
        URI.encode_query(model: state.model, language: state.language)

    case Req.post(url,
           body: audio,
           headers: [
             {"authorization", "Token #{state.api_key}"},
             {"content-type",
              "audio/raw;encoding=linear16;sample_rate=#{state.sample_rate};channels=1"}
           ]
         ) do
      {:ok, %{status: 200, body: body}} ->
        text =
          body
          |> get_in(["results", "channels"])
          |> List.first(%{})
          |> Map.get("alternatives", [])
          |> List.first(%{})
          |> Map.get("transcript", "")

        {:ok, text}

      {:ok, %{status: status, body: body}} ->
        {:error, "Deepgram API error (#{status}): #{inspect(body)}"}

      {:error, error} ->
        {:error, "Deepgram request failed: #{inspect(error)}"}
    end
  end
end
