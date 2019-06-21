defmodule JobQueue.Supervisor do
  use Supervisor

  alias JobQueue.Config
  alias JobQueue.Topic.Supervisor, as: TopicSupervisor

  require Logger

  @type option ::
          {:name, module()}
          | {:poll_interval, pos_integer()}
          | {:topics, [{atom(), map()}]}

  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link([]) do
    start_link(queue_config())
  end

  def start_link(opts) when is_list(opts) do
    conf = Config.new(opts)

    Supervisor.start_link(__MODULE__, conf, name: JobQueue.via_tuple(:supervisor, conf.name))
  end

  @impl Supervisor
  def init(%Config{topics: topics} = conf) do
    Logger.info("Starting #{__MODULE__} with topics: #{inspect(topics)}")
    children = Enum.map(topics, &topic_spec(&1, conf))

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp topic_spec({topic, topic_option}, conf) do
    topic = to_string(topic)
    limit = Config.topic_parallelism(topic_option)
    backoff_strategy = Config.topic_backoff_strategy(topic_option)
    opts = [conf: conf, topic: topic, limit: limit, backoff_strategy: backoff_strategy]

    Supervisor.child_spec({TopicSupervisor, opts}, id: JobQueue.make_id(:topic_supervisor, conf.name, topic))
  end

  defp queue_config() do
    Application.get_env(:job_queue, JobQueue)
  end
end
