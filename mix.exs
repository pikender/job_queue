defmodule JobQueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :job_queue,
      version: "0.1.0",
      elixir: "~> 1.8",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {JobQueue.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 2.2.11", override: true},
      {:sqlite_ecto2, "~> 2.4"},
      {:esqlite, github: "mmzeeman/esqlite", branch: "master", override: true},
      {:ecto_ulid, "~> 0.2.0"},
      {:jason, "~> 1.0"},
      {:gen_stage, "~> 0.14"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
     "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
     "ecto.reset": ["ecto.drop", "ecto.setup"],
     test: ["ecto.create --quiet", "ecto.migrate", "test"]
    ]
  end
end
