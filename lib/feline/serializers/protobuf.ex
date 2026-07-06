defmodule Feline.Serializers.Protobuf do
  @moduledoc """
  Wire codec for pipecat's `frames.proto`, hand-rolled (the schema is five
  flat messages in a oneof - not worth a protobuf dependency). Compatible
  with the `@pipecat-ai/websocket-transport` client.

      message Frame {
        oneof frame {
          TextFrame text = 1;             // id, name, text
          AudioRawFrame audio = 2;        // id, name, audio, sample_rate, num_channels, pts
          TranscriptionFrame transcription = 3;  // id, name, text, user_id, timestamp
          MessageFrame message = 4;       // data (JSON string)
          InterruptionFrame interruption = 5;    // id, name
        }
      }
  """

  import Bitwise

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputTransportMessageFrame,
    OutputTransportMessageUrgentFrame,
    TextFrame,
    TranscriptionFrame,
    TTSAudioRawFrame
  }

  # -- Serialize (server -> client) --

  def serialize(%TextFrame{text: text}) do
    wrap(1, field(3, :string, text))
  end

  def serialize(%module{} = frame)
      when module in [OutputAudioRawFrame, TTSAudioRawFrame, InputAudioRawFrame] do
    wrap(
      2,
      field(3, :bytes, frame.audio) <>
        field(4, :varint, frame.sample_rate) <> field(5, :varint, frame.num_channels)
    )
  end

  def serialize(%TranscriptionFrame{} = frame) do
    wrap(
      3,
      field(3, :string, frame.text) <>
        field(4, :string, frame.user_id) <> field(5, :string, frame.timestamp)
    )
  end

  def serialize(%module{message: message})
      when module in [OutputTransportMessageFrame, OutputTransportMessageUrgentFrame] do
    wrap(4, field(1, :string, Jason.encode!(message)))
  end

  def serialize(%InterruptionFrame{}), do: wrap(5, <<>>)

  def serialize(_frame), do: nil

  # -- Deserialize (client -> server) --

  def deserialize(binary) do
    case decode_fields(binary) do
      [{1, :bytes, inner}] -> decode_text(inner)
      [{2, :bytes, inner}] -> decode_audio(inner)
      [{3, :bytes, inner}] -> decode_transcription(inner)
      [{4, :bytes, inner}] -> decode_message(inner)
      [{5, :bytes, _inner}] -> %InterruptionFrame{}
      _other -> nil
    end
  end

  defp decode_text(inner) do
    fields = decode_fields(inner)
    %TextFrame{text: string_field(fields, 3)}
  end

  defp decode_audio(inner) do
    fields = decode_fields(inner)

    %InputAudioRawFrame{
      audio: bytes_field(fields, 3),
      sample_rate: varint_field(fields, 4, 16_000),
      num_channels: varint_field(fields, 5, 1)
    }
  end

  defp decode_transcription(inner) do
    fields = decode_fields(inner)

    %TranscriptionFrame{
      text: string_field(fields, 3),
      user_id: string_field(fields, 4),
      timestamp: string_field(fields, 5)
    }
  end

  defp decode_message(inner) do
    fields = decode_fields(inner)

    case Jason.decode(string_field(fields, 1)) do
      {:ok, message} -> %InputTransportMessageFrame{message: message}
      {:error, _reason} -> nil
    end
  end

  # -- proto3 wire format --

  # Unlike scalar fields, the oneof submessage is emitted even when empty.
  defp wrap(oneof_field, inner),
    do: key(oneof_field, 2) <> varint(byte_size(inner)) <> inner

  defp field(_number, :string, ""), do: <<>>
  defp field(_number, :bytes, <<>>), do: <<>>
  defp field(_number, :varint, 0), do: <<>>

  defp field(number, :varint, value), do: key(number, 0) <> varint(value)
  defp field(number, :string, value), do: key(number, 2) <> varint(byte_size(value)) <> value
  defp field(number, :bytes, value), do: key(number, 2) <> varint(byte_size(value)) <> value

  defp key(number, wire_type), do: varint(number <<< 3 ||| wire_type)

  defp varint(value) when value < 128, do: <<value>>
  defp varint(value), do: <<1::1, value::7, varint(value >>> 7)::binary>>

  defp decode_fields(binary, fields \\ [])
  defp decode_fields(<<>>, fields), do: Enum.reverse(fields)

  defp decode_fields(binary, fields) do
    {key, rest} = decode_varint(binary)
    number = key >>> 3

    case key &&& 7 do
      0 ->
        {value, rest} = decode_varint(rest)
        decode_fields(rest, [{number, :varint, value} | fields])

      2 ->
        {length, rest} = decode_varint(rest)
        <<value::binary-size(length), rest::binary>> = rest
        decode_fields(rest, [{number, :bytes, value} | fields])

      _unsupported ->
        Enum.reverse(fields)
    end
  end

  defp decode_varint(binary, shift \\ 0, acc \\ 0)

  defp decode_varint(<<0::1, value::7, rest::binary>>, shift, acc),
    do: {acc ||| value <<< shift, rest}

  defp decode_varint(<<1::1, value::7, rest::binary>>, shift, acc),
    do: decode_varint(rest, shift + 7, acc ||| value <<< shift)

  defp string_field(fields, number) do
    case List.keyfind(fields, number, 0) do
      {^number, :bytes, value} -> value
      _missing -> ""
    end
  end

  defp bytes_field(fields, number), do: string_field(fields, number)

  defp varint_field(fields, number, default) do
    case List.keyfind(fields, number, 0) do
      {^number, :varint, value} -> value
      _missing -> default
    end
  end
end
