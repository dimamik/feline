defmodule Feline.Audio.Resampler do
  @moduledoc """
  Behaviour for resampling PCM16 audio between sample rates.
  """

  @callback resample(audio :: binary(), from_rate :: pos_integer(), to_rate :: pos_integer()) ::
              binary()
end

defmodule Feline.Audio.Resampler.Linear do
  @moduledoc """
  Linear interpolation resampler for PCM16 (16-bit signed little-endian) audio.
  """

  @behaviour Feline.Audio.Resampler

  def resample(<<>>, _from_rate, _to_rate), do: <<>>
  def resample(audio, rate, rate) when is_binary(audio), do: audio

  def resample(audio, from_rate, to_rate)
      when is_binary(audio) and is_integer(from_rate) and is_integer(to_rate) do
    samples = decode_samples(audio, [])
    input_len = length(samples)
    output_len = max(1, div(input_len * to_rate, from_rate))
    input_array = :array.from_list(samples)

    if output_len == 1 do
      sample = :array.get(0, input_array)
      <<sample::little-signed-16>>
    else
      ratio = (input_len - 1) / (output_len - 1)
      build_output(input_array, input_len, output_len, ratio, 0, <<>>)
    end
  end

  defp build_output(_input, _input_len, output_len, _ratio, i, acc) when i >= output_len, do: acc

  defp build_output(input, input_len, output_len, ratio, i, acc) do
    pos = i * ratio
    idx = trunc(pos)
    frac = pos - idx

    sample =
      if idx >= input_len - 1 do
        :array.get(input_len - 1, input)
      else
        s0 = :array.get(idx, input)
        s1 = :array.get(idx + 1, input)
        round(s0 + (s1 - s0) * frac)
      end

    sample = max(-32_768, min(32_767, sample))

    build_output(
      input,
      input_len,
      output_len,
      ratio,
      i + 1,
      <<acc::binary, sample::little-signed-16>>
    )
  end

  defp decode_samples(<<sample::little-signed-16, rest::binary>>, acc) do
    decode_samples(rest, [sample | acc])
  end

  defp decode_samples(<<>>, acc), do: Enum.reverse(acc)
  defp decode_samples(<<_::binary-size(1)>>, acc), do: Enum.reverse(acc)
end
