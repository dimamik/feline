defmodule Feline.Services.TTS do
  @moduledoc """
  Behaviour for text-to-speech services. Processes text frames and
  produces audio output frames.
  """

  alias Feline.Frames.{TextFrame, LLMTextFrame, TTSStartedFrame, TTSStoppedFrame}

  @callback run_tts(text :: String.t(), state :: term()) ::
              {:ok, audio_frames :: [struct()], state :: term()}

  defmacro __using__(_opts) do
    quote do
      use Feline.Processor

      @behaviour Feline.Services.TTS

      @impl Feline.Processor
      def handle_frame(%frame_mod{text: text}, :downstream, _push_fn, state)
          when frame_mod in [TextFrame, LLMTextFrame] do
        {:ok, audio_frames, new_state} = run_tts(text, state)

        out =
          [{%TTSStartedFrame{id: make_ref()}, :downstream}] ++
            Enum.map(audio_frames, &{&1, :downstream}) ++
            [{%TTSStoppedFrame{id: make_ref()}, :downstream}]

        {:push_many, out, new_state}
      end

      def handle_frame(frame, direction, _push_fn, state) do
        {:push, frame, direction, state}
      end

      defoverridable handle_frame: 4
    end
  end
end
