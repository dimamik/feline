defmodule Feline.MixProject do
  use Mix.Project

  @version "0.1.0-rc.1"

  def project do
    [
      app: :feline,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: "Real-time voice and multimodal AI pipelines for Elixir, inspired by pipecat",
      package: package(),
      source_url: "https://github.com/dimamik/feline",
      homepage_url: "https://github.com/dimamik/feline",
      aliases: aliases(),
      docs: [
        main: "readme",
        extras: ["README.md", "CHANGELOG.md", "guides/live-voice-demo.md"],
        groups_for_extras: [Guides: ~r/guides\/.*/],
        assets: %{"assets" => "assets"}
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Feline.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/dimamik/feline",
        "pipecat" => "https://github.com/pipecat-ai/pipecat"
      },
      maintainers: ["Dima Mikielewicz"],
      files: ~w(lib assets .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp aliases do
    [
      release: [
        "cmd git tag v#{@version}",
        "cmd git push",
        "cmd git push --tags",
        "hex.publish --yes"
      ]
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:req, "~> 0.5"},
      {:websockex, "~> 0.4"},
      {:bandit, "~> 1.6"},
      {:websock_adapter, "~> 0.5"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, ">= 0.0.0", only: [:dev, :test], runtime: false}
    ]
  end
end
