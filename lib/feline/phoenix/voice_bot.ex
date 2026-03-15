if Code.ensure_loaded?(Phoenix.Component) do
  defmodule Feline.Phoenix.VoiceBot do
    @moduledoc """
    LiveView function component for embedding a voice AI bot.

    Renders a container `<div>` with a `phx-hook="FelineVoiceBot"` that
    the JS hook uses to initialize pipecat-client-web with a custom
    WebSocket transport pointing at `ws_url`.

    ## Usage

        import Feline.Phoenix.VoiceBot

        def render(assigns) do
          ~H\"\"\"
          <.voice_bot id="my-bot" ws_url={~p"/feline/ws"}>
            <button phx-click="start_voice">Start</button>
          </.voice_bot>
          \"\"\"
        end
    """
    use Phoenix.Component

    attr(:id, :string, required: true)
    attr(:ws_url, :string, required: true)
    attr(:class, :string, default: "")
    slot(:inner_block, required: false)

    def voice_bot(assigns) do
      ~H"""
      <div id={@id} phx-hook="FelineVoiceBot" data-ws-url={@ws_url} class={@class}>
        <%= render_slot(@inner_block) %>
      </div>
      """
    end
  end
end
