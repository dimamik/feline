defmodule Feline.TransportParams do
  @moduledoc """
  Configuration for WebSocket transport audio settings and session timeout.
  """
  defstruct audio_in_enabled: true,
            audio_in_sample_rate: 16_000,
            audio_out_enabled: true,
            audio_out_sample_rate: 24_000,
            audio_out_10ms_chunks: 4,
            audio_out_end_silence_secs: 2.0,
            session_timeout_secs: nil
end
