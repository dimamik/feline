defmodule Feline.Services.STT do
  @moduledoc """
  Behaviour for speech-to-text services. Processes audio frames and
  produces transcription frames.
  """

  alias Feline.Frames.InputAudioRawFrame

  @callback run_stt(audio :: binary(), state :: term()) ::
              {:ok, output_frames :: [struct()], state :: term()}
              | {:continue, state :: term()}

  defmacro __using__(_opts) do
    quote do
      use Feline.Processor

      @behaviour Feline.Services.STT

      @impl Feline.Processor
      def handle_frame(%InputAudioRawFrame{audio: audio} = frame, :downstream, _push_fn, state) do
        case run_stt(audio, state) do
          {:ok, transcription_frames, new_state} ->
            out =
              [{frame, :downstream} | Enum.map(transcription_frames, &{&1, :downstream})]

            {:push_many, out, new_state}

          {:continue, new_state} ->
            {:push, frame, :downstream, new_state}
        end
      end

      def handle_frame(frame, direction, _push_fn, state) do
        {:push, frame, direction, state}
      end

      defoverridable handle_frame: 4
    end
  end
end
