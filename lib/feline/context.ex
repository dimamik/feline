defmodule Feline.Context do
  @moduledoc """
  LLM conversation context. Holds the messages list, available tools,
  and tool choice configuration.

  Messages are stored in reverse order internally for O(1) appends.
  Use `messages/1` to get them in chronological order.
  """

  defstruct messages: [],
            tools: [],
            tool_choice: :auto

  def new(messages \\ []) do
    %__MODULE__{messages: Enum.reverse(messages)}
  end

  def messages(%__MODULE__{messages: reversed}) do
    Enum.reverse(reversed)
  end

  def append_message(%__MODULE__{} = ctx, message) do
    %{ctx | messages: [message | ctx.messages]}
  end

  def append_messages(%__MODULE__{} = ctx, new_messages) do
    reversed_new = Enum.reverse(new_messages)
    %{ctx | messages: reversed_new ++ ctx.messages}
  end

  def set_messages(%__MODULE__{} = ctx, messages) do
    %{ctx | messages: Enum.reverse(messages)}
  end
end
