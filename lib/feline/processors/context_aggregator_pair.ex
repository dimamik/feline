defmodule Feline.Processors.ContextAggregatorPair do
  @moduledoc """
  Creates a shared context between UserContextAggregator and AssistantContextAggregator.
  Both aggregators read/write through the same Agent, keeping conversation history in sync.

  ## Usage

      context = Feline.Context.new([%{role: "system", content: "You are helpful."}])
      {:ok, pair} = ContextAggregatorPair.start(context)

      pipeline = %Pipeline{
        processor_specs: [
          {UserContextAggregator, context_agent: pair.agent},
          {StreamingLLM, api_key: "..."},
          {AssistantContextAggregator, context_agent: pair.agent}
        ]
      }
  """

  alias Feline.Context

  defstruct [:agent]

  def start(context \\ Context.new()) do
    {:ok, agent} = Agent.start_link(fn -> context end)
    {:ok, %__MODULE__{agent: agent}}
  end

  def get_context(agent) do
    Agent.get(agent, & &1)
  end

  def update_context(agent, fun) do
    Agent.update(agent, fun)
  end

  def append_message(agent, message) do
    Agent.update(agent, &Context.append_message(&1, message))
    Agent.get(agent, & &1)
  end
end
