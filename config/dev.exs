use Mix.Config

config :job_queue, JobQueue.Repo,
  adapter: Sqlite.Ecto2,
  database: "_state/dev.sqlite3",
  pool: Ecto.Adapters.SQL.Sandbox
