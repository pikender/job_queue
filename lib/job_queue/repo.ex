defmodule JobQueue.Repo do
  use Ecto.Repo,
    otp_app: :job_queue,
    adapter: Sqlite.Ecto2

  require Logger

  def init(_context, config) do
    {:ok, db_path_with_override(config)}
  end

  # During the build phase the Dockerfile sets a temporary DB path override
  # which let's us run the migrations on a template db that will be embedded in
  # the docker image and copied onto the permanent/external volume on first-boot
  defp db_path_with_override(config) do
    case System.get_env("JOBQUEUE_DB_PATH") do
      nil ->
        config

      path when is_binary(path) ->
        Logger.info("JOBQUEUE_DB_PATH database override found: #{path}")
        Keyword.put(config, :database, path)
    end
  end
end
