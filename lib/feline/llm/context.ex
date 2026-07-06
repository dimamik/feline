defmodule Feline.LLM.Context do
  @moduledoc "Conversation context: OpenAI-shaped message maps plus tool definitions."

  defstruct messages: [], tools: []

  def new(opts \\ []) do
    %__MODULE__{
      messages: Keyword.get(opts, :messages, []),
      tools: Keyword.get(opts, :tools, [])
    }
  end

  def append(context, messages) when is_list(messages),
    do: %{context | messages: context.messages ++ messages}

  def append(context, message), do: append(context, [message])
end
