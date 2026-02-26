defmodule Feline.Audio.Utils do
  @moduledoc """
  Pure Elixir audio utilities for PCM16 (16-bit signed little-endian) audio.
  """

  @speaking_threshold 20

  @doc "Returns true if audio is silence (max amplitude <= threshold)"
  def silence?(audio, threshold \\ @speaking_threshold) when is_binary(audio) do
    max_amplitude(audio) <= threshold
  end

  @doc "Returns the maximum absolute amplitude in PCM16 audio"
  def max_amplitude(audio) when is_binary(audio) do
    samples_reduce(audio, 0, fn sample, acc -> max(abs(sample), acc) end)
  end

  @doc "Compute RMS (root mean square) amplitude of PCM16 audio"
  def compute_rms(audio) when is_binary(audio) do
    {sum_sq, count} =
      samples_reduce(audio, {0.0, 0}, fn sample, {sum, n} ->
        {sum + sample * sample, n + 1}
      end)

    if count == 0, do: 0.0, else: :math.sqrt(sum_sq / count)
  end

  @doc "Mix two PCM16 audio binaries (sample-by-sample addition with clipping to int16 range)"
  def mix_audio(audio1, audio2) when is_binary(audio1) and is_binary(audio2) do
    len = max(byte_size(audio1), byte_size(audio2))
    a1 = pad_audio(audio1, len)
    a2 = pad_audio(audio2, len)

    mix_samples(a1, a2, <<>>)
  end

  @doc "Generate silence (zero-filled PCM16) for given duration"
  def generate_silence(duration_ms, sample_rate, num_channels \\ 1) do
    num_samples = div(sample_rate * duration_ms, 1_000) * num_channels
    <<0::size(num_samples * 16)>>
  end

  @doc "Calculate audio duration in milliseconds"
  def duration_ms(audio, sample_rate, num_channels \\ 1) when is_binary(audio) do
    num_samples = div(byte_size(audio), 2 * num_channels)
    div(num_samples * 1_000, sample_rate)
  end

  # -- Private helpers --

  defp samples_reduce(<<sample::little-signed-16, rest::binary>>, acc, fun) do
    samples_reduce(rest, fun.(sample, acc), fun)
  end

  defp samples_reduce(<<>>, acc, _fun), do: acc
  defp samples_reduce(<<_::binary-size(1)>>, acc, _fun), do: acc

  defp mix_samples(
         <<s1::little-signed-16, r1::binary>>,
         <<s2::little-signed-16, r2::binary>>,
         acc
       ) do
    mixed = max(-32_768, min(32_767, s1 + s2))
    mix_samples(r1, r2, <<acc::binary, mixed::little-signed-16>>)
  end

  defp mix_samples(<<>>, <<>>, acc), do: acc

  defp pad_audio(audio, target_len) when byte_size(audio) >= target_len, do: audio

  defp pad_audio(audio, target_len) do
    pad_size = target_len - byte_size(audio)
    audio <> <<0::size(pad_size * 8)>>
  end
end
