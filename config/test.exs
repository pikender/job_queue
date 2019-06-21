use Mix.Config

config :job_queue,
  ecto_repos: [JobQueue.Repo]

config :job_queue, JobQueue.Repo,
  adapter: Sqlite.Ecto2,
  database: "_state/test.sqlite3",
  pool: Ecto.Adapters.SQL.Sandbox
