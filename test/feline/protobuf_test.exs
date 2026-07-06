defmodule Feline.ProtobufTest do
  use ExUnit.Case, async: true

  alias Feline.Serializers.Protobuf

  alias Feline.Frames.All.{
    InputAudioRawFrame,
    InputTransportMessageFrame,
    InterruptionFrame,
    OutputTransportMessageUrgentFrame,
    TextFrame,
    TranscriptionFrame,
    TTSAudioRawFrame
  }

  # Golden bytes generated with pipecat's Python frames_pb2 (protoc gencode).
  @golden_text Base.decode16!("0a071a0568656c6c6f", case: :lower)
  @golden_audio Base.decode16!("120c1a04010203fa20c0bb012801", case: :lower)
  @golden_transcription Base.decode16!(
                          "1a241a086869207468657265220275312a14323032362d30372d30325430303a30303a30305a",
                          case: :lower
                        )
  @golden_message Base.decode16!(
                    "224e0a4c7b226c6162656c223a22727476692d6169222c2274797065223a22626f742d7265616479222c226964223a22616263222c2264617461223a7b2276657273696f6e223a22322e302e30227d7d",
                    case: :lower
                  )
  @golden_interruption Base.decode16!("2a00", case: :lower)

  test "serializes frames byte-identically to Python's encoder" do
    assert Protobuf.serialize(%TextFrame{text: "hello"}) == @golden_text

    assert Protobuf.serialize(%TTSAudioRawFrame{
             audio: <<1, 2, 3, 250>>,
             sample_rate: 24_000,
             num_channels: 1
           }) == @golden_audio

    assert Protobuf.serialize(%TranscriptionFrame{
             text: "hi there",
             user_id: "u1",
             timestamp: "2026-07-02T00:00:00Z"
           }) == @golden_transcription

    assert Protobuf.serialize(%InterruptionFrame{}) == @golden_interruption
  end

  test "serializes transport messages with JSON payloads" do
    frame = %OutputTransportMessageUrgentFrame{
      message: %{
        "label" => "rtvi-ai",
        "type" => "bot-ready",
        "id" => "abc",
        "data" => %{"version" => "2.0.0"}
      }
    }

    # Key order in JSON differs; compare after decoding the payload.
    assert %InputTransportMessageFrame{message: message} =
             frame |> Protobuf.serialize() |> Protobuf.deserialize()

    assert %InputTransportMessageFrame{message: ^message} =
             Protobuf.deserialize(@golden_message)
  end

  test "deserializes Python-encoded frames" do
    assert Protobuf.deserialize(@golden_text) == %TextFrame{text: "hello"}

    assert Protobuf.deserialize(@golden_audio) == %InputAudioRawFrame{
             audio: <<1, 2, 3, 250>>,
             sample_rate: 24_000,
             num_channels: 1
           }

    assert Protobuf.deserialize(@golden_transcription) == %TranscriptionFrame{
             text: "hi there",
             user_id: "u1",
             timestamp: "2026-07-02T00:00:00Z"
           }

    assert Protobuf.deserialize(@golden_interruption) == %InterruptionFrame{}
  end

  test "audio round-trips through serialize/deserialize" do
    audio = :crypto.strong_rand_bytes(3_200)

    assert %InputAudioRawFrame{audio: ^audio, sample_rate: 16_000, num_channels: 1} =
             %TTSAudioRawFrame{audio: audio, sample_rate: 16_000, num_channels: 1}
             |> Protobuf.serialize()
             |> Protobuf.deserialize()
  end

  test "garbage input deserializes to nil, not a crash" do
    assert Protobuf.deserialize(<<255, 1, 2>>) == nil
  end
end
