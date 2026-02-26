defmodule Feline.Test.Collector do
  @moduledoc false
  use Feline.Processor

  @impl true
  def init(opts), do: {:ok, %{test_pid: Keyword.fetch!(opts, :test_pid)}}

  @impl true
  def handle_frame(frame, direction, _push_fn, state) do
    send(state.test_pid, {:frame, frame})
    {:push, frame, direction, state}
  end
end

defmodule Feline.Test.IntegrationHelper do
  @moduledoc false

  def require_env!(key) do
    case System.get_env(key) do
      nil -> raise "Missing #{key} — add it to .env"
      "" -> raise "Empty #{key} — add it to .env"
      val -> val
    end
  end

  @doc """
  Plays raw PCM audio through macOS `afplay` by writing a temporary WAV file.
  Blocks until playback finishes.
  """
  def play_pcm!(pcm_data, sample_rate \\ 24_000) do
    path = Path.join(System.tmp_dir!(), "feline_test_#{:erlang.unique_integer([:positive])}.wav")
    File.write!(path, wav_header(pcm_data, sample_rate) <> pcm_data)
    System.cmd("afplay", [path])
    File.rm(path)
  end

  defp wav_header(pcm_data, sample_rate) do
    channels = 1
    bits_per_sample = 16
    byte_rate = sample_rate * channels * div(bits_per_sample, 8)
    block_align = channels * div(bits_per_sample, 8)
    data_size = byte_size(pcm_data)

    <<
      "RIFF",
      data_size + 36::little-32,
      "WAVE",
      "fmt ",
      16::little-32,
      1::little-16,
      channels::little-16,
      sample_rate::little-32,
      byte_rate::little-32,
      block_align::little-16,
      bits_per_sample::little-16,
      "data",
      data_size::little-32
    >>
  end
end
