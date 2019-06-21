defmodule JobQueue.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      JobQueue.Repo,
      {JobQueue.Events, []},
      {Registry, keys: :unique, name: JobQueue.Registry}
    ]

    opts = [strategy: :one_for_one, name: JobQueue.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
