defmodule Feline.Services.OpenAI.LLM do
  @moduledoc """
  OpenAI LLM service. Sends conversation context to the OpenAI Chat
  Completions API and streams back LLMTextFrame responses.
  """
  use Feline.Services.LLM

  alias Feline.Frames.LLMTextFrame

  @default_model "gpt-4.1"
  @default_base_url "https://api.openai.com/v1"

  @impl Feline.Processor
  def init(opts) do
    {:ok,
     %{
       api_key: Keyword.fetch!(opts, :api_key),
       model: Keyword.get(opts, :model, @default_model),
       base_url: Keyword.get(opts, :base_url, @default_base_url),
       temperature: Keyword.get(opts, :temperature),
       max_tokens: Keyword.get(opts, :max_tokens)
     }}
  end

  @impl Feline.Services.LLM
  def process_context(context, state) do
    body =
      %{
        model: state.model,
        messages: Feline.Context.messages(context),
        stream: false
      }
      |> maybe_put(:temperature, state.temperature)
      |> maybe_put(:max_tokens, state.max_tokens)
      |> maybe_put_tools(context.tools)

    case Req.post(
           "#{state.base_url}/chat/completions",
           json: body,
           headers: [
             {"authorization", "Bearer #{state.api_key}"},
             {"content-type", "application/json"}
           ]
         ) do
      {:ok, %{status: 200, body: %{"choices" => [%{"message" => %{"content" => content}} | _]}}} ->
        frame = %LLMTextFrame{id: make_ref(), text: content || ""}
        {:ok, [frame], state}

      {:ok, %{status: status, body: body}} ->
        raise "OpenAI API error (#{status}): #{inspect(body)}"

      {:error, error} ->
        raise "OpenAI API request failed: #{inspect(error)}"
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_tools(map, []), do: map
  defp maybe_put_tools(map, tools), do: Map.put(map, :tools, tools)
end
