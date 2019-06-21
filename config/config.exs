use Mix.Config

config :job_queue,
  ecto_repos: [JobQueue.Repo]

config :ecto, :json_library, Jason

import_config "#{Mix.env()}.exs"
