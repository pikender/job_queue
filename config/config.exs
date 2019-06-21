use Mix.Config

config :job_queue,
  data_dir: Path.expand("../_state/test/data", __DIR__)

config :ecto, :json_library, Jason

import_config "#{Mix.env()}.exs"
