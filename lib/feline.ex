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
        {MyApp.STT, api_key: "..."},
        {MyApp.LLM, api_key: "..."},
        {MyApp.TTS, api_key: "..."}
      ])

      Feline.Pipeline.Runner.run(pipeline)
  """
end
