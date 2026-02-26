defmodule Feline.FrameTest do
  use ExUnit.Case, async: true

  alias Feline.Frame

  alias Feline.Frames.{
    StartFrame,
    CancelFrame,
    EndFrame,
    ErrorFrame,
    InterruptionFrame,
    InputImageRawFrame,
    UserImageRawFrame,
    TextFrame,
    LLMTextFrame,
    OutputAudioRawFrame,
    OutputImageRawFrame,
    SpriteFrame,
    VisionTextFrame,
    HeartbeatFrame,
    FunctionCallResultFrame
  }

  describe "Frame.system?/1" do
    test "returns true for system frames" do
      assert Frame.system?(%StartFrame{id: make_ref()})
      assert Frame.system?(%CancelFrame{id: make_ref()})
      assert Frame.system?(%ErrorFrame{id: make_ref(), message: "oops"})
      assert Frame.system?(%InterruptionFrame{id: make_ref()})
    end

    test "returns false for data frames" do
      refute Frame.system?(%TextFrame{id: make_ref(), text: "hello"})
      refute Frame.system?(%LLMTextFrame{id: make_ref(), text: "hi"})
      refute Frame.system?(%OutputAudioRawFrame{id: make_ref(), audio: <<>>, sample_rate: 16_000})

      refute Frame.system?(%OutputImageRawFrame{
               id: make_ref(),
               image: <<>>,
               width: 100,
               height: 100
             })

      refute Frame.system?(%SpriteFrame{id: make_ref(), images: []})

      refute Frame.system?(%VisionTextFrame{
               id: make_ref(),
               text: "hi",
               image: <<>>,
               width: 100,
               height: 100
             })

      refute Frame.system?(%InputImageRawFrame{
               id: make_ref(),
               image: <<>>,
               width: 100,
               height: 100
             })

      refute Frame.system?(%UserImageRawFrame{
               id: make_ref(),
               image: <<>>,
               width: 100,
               height: 100,
               user_id: "u1"
             })
    end

    test "returns false for control frames" do
      refute Frame.system?(%EndFrame{id: make_ref()})
      refute Frame.system?(%HeartbeatFrame{id: make_ref()})
    end
  end

  describe "Frame.uninterruptible?/1" do
    test "EndFrame is uninterruptible" do
      assert Frame.uninterruptible?(%EndFrame{id: make_ref()})
    end

    test "FunctionCallResultFrame is uninterruptible" do
      assert Frame.uninterruptible?(%FunctionCallResultFrame{
               id: make_ref(),
               function_name: "f",
               tool_call_id: "t",
               result: "r"
             })
    end

    test "TextFrame is interruptible" do
      refute Frame.uninterruptible?(%TextFrame{id: make_ref(), text: "hi"})
    end

    test "StartFrame is interruptible" do
      refute Frame.uninterruptible?(%StartFrame{id: make_ref()})
    end

    test "InputImageRawFrame is interruptible" do
      refute Frame.uninterruptible?(%InputImageRawFrame{
               id: make_ref(),
               image: <<>>,
               width: 100,
               height: 100
             })
    end

    test "OutputImageRawFrame is interruptible" do
      refute Frame.uninterruptible?(%OutputImageRawFrame{
               id: make_ref(),
               image: <<>>,
               width: 100,
               height: 100
             })
    end
  end

  describe "Frame.new_id/0" do
    test "returns a reference" do
      id = Frame.new_id()
      assert is_reference(id)
    end

    test "returns unique values" do
      id1 = Frame.new_id()
      id2 = Frame.new_id()
      assert id1 != id2
    end
  end
end
