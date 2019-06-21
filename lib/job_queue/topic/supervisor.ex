defmodule JobQueue.Topic.Supervisor do
  @moduledoc false

  use Supervisor

  alias JobQueue.Config
  alias JobQueue.Topic.Producer

  @type option ::
          {:name, module()}
          | {:conf, Config.t()}
          | {:topic, binary()}
          | {:limit, pos_integer()}
          | {:backoff_strategy, atom()}

  @spec start_link([option]) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    topic = Keyword.get(opts, :topic)

    Supervisor.start_link(__MODULE__, opts, name: JobQueue.via_tuple(:topic_supervisor, topic))
  end

  @impl Supervisor
  def init(opts) do
    conf = Keyword.get(opts, :conf)
    limit = Keyword.get(opts, :limit)
    topic = Keyword.get(opts, :topic)
    backoff_strategy = Keyword.get(opts, :backoff_strategy)

    fore_name = JobQueue.process_name({:topic_foreman, conf.name}, topic)

    prod_opts = [
      conf: conf,
      foreman: fore_name,
      limit: limit,
      topic: topic,
      backoff_strategy: backoff_strategy
    ]

    children = [
      {Task.Supervisor, name: fore_name},
      {Producer, prod_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
