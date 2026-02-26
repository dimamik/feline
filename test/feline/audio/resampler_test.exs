defmodule Feline.Audio.Resampler.LinearTest do
  use ExUnit.Case, async: true

  alias Feline.Audio.Resampler.Linear

  defp sample_count(audio), do: div(byte_size(audio), 2)

  defp encode_samples(samples) do
    for s <- samples, into: <<>>, do: <<s::little-signed-16>>
  end

  describe "resample/3" do
    test "same rate returns unchanged audio" do
      audio = encode_samples([100, 200, 300, 400])
      assert Linear.resample(audio, 16_000, 16_000) == audio
    end

    test "empty binary returns empty binary" do
      assert Linear.resample(<<>>, 16_000, 24_000) == <<>>
    end

    test "single sample edge case" do
      audio = encode_samples([500])

      # Same rate preserves single sample
      assert Linear.resample(audio, 16_000, 16_000) == audio

      # Upsampling single sample: output samples all equal the input
      result = Linear.resample(audio, 8_000, 16_000)
      decoded = decode_samples(result)
      assert Enum.all?(decoded, &(&1 == 500))
    end

    test "upsampling 8000 -> 16000 doubles sample count" do
      samples = for i <- 0..99, do: i * 100
      audio = encode_samples(samples)

      result = Linear.resample(audio, 8_000, 16_000)
      assert sample_count(result) == 200
    end

    test "downsampling 16000 -> 8000 halves sample count" do
      samples = for i <- 0..99, do: i * 100
      audio = encode_samples(samples)

      result = Linear.resample(audio, 16_000, 8_000)
      assert sample_count(result) == 50
    end

    test "upsampled output preserves first and last samples" do
      samples = [0, 1000, 2000, 3000]
      audio = encode_samples(samples)

      result = Linear.resample(audio, 8_000, 16_000)
      decoded = decode_samples(result)

      assert hd(decoded) == 0
      assert List.last(decoded) == 3000
    end

    test "linear interpolation produces expected midpoints" do
      audio = encode_samples([0, 1000])

      result = Linear.resample(audio, 8_000, 24_000)
      decoded = decode_samples(result)

      assert hd(decoded) == 0
      assert List.last(decoded) == 1000
      # middle sample should be interpolated between 0 and 1000
      mid = Enum.at(decoded, 1)
      assert mid > 0 and mid < 1000
    end
  end

  defp decode_samples(audio), do: decode_samples(audio, [])

  defp decode_samples(<<s::little-signed-16, rest::binary>>, acc),
    do: decode_samples(rest, acc ++ [s])

  defp decode_samples(<<>>, acc), do: acc
end
