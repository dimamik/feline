defmodule Feline.Processors.AudioPlayer do
  @moduledoc """
  Processor that plays TTS audio in real-time via sox `play`.

  Buffers audio per utterance. On TTSStoppedFrame, spawns a background
  task to play the buffered audio, freeing the pipeline to continue.
  On interruption, discards the buffer and kills any playing task.
  """
  use Feline.Processor

  alias Feline.Frames.{
    TTSAudioRawFrame,
    TTSStoppedFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    EndFrame,
    InterruptionFrame
  }

  @impl true
  def init(opts) do
    {:ok,
     %{
       speaking: false,
       audio_buffer: <<>>,
       play_pid: nil,
       sample_rate: Keyword.get(opts, :sample_rate, 24_000)
     }}
  end

  @impl true
  def handle_frame(%TTSAudioRawFrame{audio: audio} = frame, :downstream, push_fn, state) do
    state =
      unless state.speaking do
        push_fn.(%BotStartedSpeakingFrame{id: make_ref()}, :upstream)
        %{state | speaking: true}
      else
        state
      end

    new_buf = state.audio_buffer <> audio
    {:push, frame, :downstream, %{state | audio_buffer: new_buf}}
  end

  def handle_frame(%TTSStoppedFrame{} = frame, :downstream, _push_fn, state) do
    state = play_buffer(state)
    # BotStoppedSpeakingFrame is deferred until playback finishes (handle_info :DOWN)
    {:push, frame, :downstream, state}
  end

  def handle_frame(%InterruptionFrame{} = frame, direction, push_fn, state) do
    state = kill_playback(state)
    state = stop_speaking(state, push_fn)
    {:push, frame, direction, %{state | audio_buffer: <<>>}}
  end

  def handle_frame(%EndFrame{} = frame, direction, push_fn, state) do
    state = play_buffer(state)
    state = stop_speaking(state, push_fn)
    {:push, frame, direction, state}
  end

  def handle_frame(frame, direction, _push_fn, state) do
    {:push, frame, direction, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, push_fn, %{play_pid: pid} = state) do
    state = stop_speaking(%{state | play_pid: nil}, push_fn)
    {:ok, state}
  end

  def handle_info(_msg, _push_fn, state), do: {:ok, state}

  @impl true
  def handle_cleanup(state) do
    kill_playback(state)
    :ok
  end

  defp stop_speaking(%{speaking: false} = state, _push_fn), do: state

  defp stop_speaking(state, push_fn) do
    push_fn.(%BotStoppedSpeakingFrame{id: make_ref()}, :upstream)
    %{state | speaking: false}
  end

  defp play_buffer(%{audio_buffer: <<>>} = state), do: state

  defp play_buffer(state) do
    state = kill_playback(state)
    audio = state.audio_buffer
    sr = state.sample_rate

    pid =
      spawn(fn ->
        path =
          Path.join(System.tmp_dir!(), "feline_tts_#{:erlang.unique_integer([:positive])}.raw")

        File.write!(path, audio)

        System.cmd("play", [
          "-q",
          "-t",
          "raw",
          "-r",
          to_string(sr),
          "-e",
          "signed",
          "-b",
          "16",
          "-c",
          "1",
          path
        ])

        File.rm(path)
      end)

    Process.monitor(pid)
    %{state | audio_buffer: <<>>, play_pid: pid}
  end

  defp kill_playback(%{play_pid: nil} = state), do: state

  defp kill_playback(%{play_pid: pid} = state) do
    Process.exit(pid, :kill)
    %{state | play_pid: nil}
  end
end
