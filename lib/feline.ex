defmodule Feline do
  @moduledoc """
  Feline — Real-time voice AI pipelines for Elixir.

  An Elixir reimplementation of Python's pipecat framework, leveraging
  BEAM/OTP for true concurrent frame processing.

  ## Core Concepts

  - **Frames** — Data units flowing through the pipeline (audio, text, control signals)
  - **Processors** — GenServer processes that transform frames
  - **Pipeline** — A chain of processors linked together
  - **Services** — AI integrations (LLM, STT, TTS) implemented as processors

  ## Quick Start

      pipeline = Feline.Pipeline.new([
        {Feline.Services.Deepgram.STT, api_key: "...", sample_rate: 16_000},
        {Feline.Services.OpenAI.LLM, api_key: "...", model: "gpt-4.1-mini"},
        {Feline.Services.ElevenLabs.TTS, api_key: "...", voice_id: "..."}
      ])

      Feline.Pipeline.Runner.run(pipeline)
  """
end
