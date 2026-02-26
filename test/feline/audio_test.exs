defmodule Feline.Audio.UtilsTest do
  use ExUnit.Case, async: true

  alias Feline.Audio.Utils

  describe "silence?/2" do
    test "silence returns true for zero audio" do
      silence = <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert Utils.silence?(silence)
    end

    test "loud audio is not silence" do
      loud = <<232, 3, 232, 3>>
      refute Utils.silence?(loud)
    end
  end

  describe "compute_rms/1" do
    test "rms of silence is 0" do
      assert Utils.compute_rms(<<0, 0, 0, 0>>) == 0.0
    end

    test "rms of uniform samples" do
      audio = <<100, 0, 100, 0>>
      assert Utils.compute_rms(audio) == 100.0
    end
  end

  describe "mix_audio/2" do
    test "mixing silence with audio returns audio" do
      audio = <<100, 0, 200, 0>>
      silence = <<0, 0, 0, 0>>
      assert Utils.mix_audio(audio, silence) == audio
    end

    test "mixing clips to int16 range" do
      max_val = <<255, 127, 255, 127>>
      result = Utils.mix_audio(max_val, max_val)
      assert result == <<255, 127, 255, 127>>
    end
  end

  describe "generate_silence/3" do
    test "generates correct length" do
      silence = Utils.generate_silence(100, 16_000)
      assert byte_size(silence) == 3200
    end
  end

  describe "duration_ms/3" do
    test "calculates duration correctly" do
      audio = Utils.generate_silence(100, 16_000)
      assert Utils.duration_ms(audio, 16_000) == 100
    end
  end
end

defmodule Feline.Audio.VAD.EnergyTest do
  use ExUnit.Case, async: true

  alias Feline.Audio.VAD.Energy

  test "detects silence" do
    vad = Energy.new(threshold: 50.0)
    silence = <<0, 0, 0, 0, 0, 0, 0, 0>>
    assert {:quiet, _} = Energy.analyze(silence, 16_000, vad)
  end

  test "detects speech" do
    vad = Energy.new(threshold: 50.0)
    loud = <<232, 3, 232, 3, 232, 3, 232, 3>>
    assert {:speaking, _} = Energy.analyze(loud, 16_000, vad)
  end
end
