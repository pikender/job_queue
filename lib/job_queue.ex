defmodule JobQueue do
  @moduledoc false

  @type topic_name :: binary()

  import Ecto.Query
  import DateTime, only: [utc_now: 0]

  alias JobQueue.Job
  alias JobQueue.Repo

  defdelegate start_link(opts), to: JobQueue.Supervisor
  defdelegate child_spec(opts), to: JobQueue.Supervisor

  @spec get_available_jobs(binary(), pos_integer()) :: [Job.t()]
  def get_available_jobs(topic, demand) do
    {_, jobs} = get_available_jobs_and_mark_executing(topic, demand)
    jobs || []
  end

  @spec get_available_jobs_and_mark_executing(binary(), pos_integer()) :: {integer(), nil | [Job.t()]}
  defp get_available_jobs_and_mark_executing(topic, demand) do
    query =
      Job
      |> where([j], j.state == "available")
      |> where([j], j.topic == ^topic)
      |> where([j], j.scheduled_at <= ^utc_now())
      |> limit(^demand)
      |> order_by([j], desc: j.priority, asc: j.scheduled_at, asc: j.id)
      |> select([j], j.id)

    ids = Repo.all(query, log: false)

    Job
    |> where([j], j.id in ^ids)
    |> Repo.update_all(
      [
        set: [state: "executing", attempted_at: utc_now()],
        inc: [attempt: 1]
      ],
      [returning: true, log: false]
    )
  end

  @spec stage_scheduled_jobs(binary()) :: {integer(), nil}
  def stage_scheduled_jobs(topic) do
    Job
    |> where([j], j.state in ["scheduled", "retryable"])
    |> where([j], j.topic == ^topic)
    |> where([j], j.scheduled_at <= ^utc_now())
    |> Repo.update_all([set: [state: "available"]], [log: false])
  end

  @doc """
  Helps with the app restarts and assuming that executing
  jobs would be from last run and may be orphaned
  Due to absence of any locks, this is an optimistic approach
  which could result in multiple job runs when process running
  job is failing frequently
  """
  @spec rescue_orphaned_jobs(binary()) :: {integer(), nil}
  def rescue_orphaned_jobs(topic) do
    Job
    |> where([j], j.state in ["executing", "launched"])
    |> where([j], j.topic == ^topic)
    |> Repo.update_all([set: [state: "available"]], [returning: true])
  end

  @spec delete_outdated_jobs(pos_integer()) :: {integer(), nil}
  def delete_outdated_jobs(seconds) do
    outdated_at = DateTime.add(utc_now(), -seconds)

    Job
    |> where([j], j.state == "completed" and j.completed_at < ^outdated_at)
    |> or_where([j], j.state == "discarded" and j.attempted_at < ^outdated_at)
    |> Repo.delete_all()
  end

  @spec complete_job(Job.t()) :: :ok
  def complete_job(%Job{} = job) do
    update_state(job, "completed")
  end

  @spec discard_job(Job.t()) :: :ok
  def discard_job(%Job{} = job) do
    update_state(job, "discarded")
  end

  def update_state(%Job{id: id}, state) when state in ["completed", "discarded"] do
    {1, [job]} =
      Job
      |> where(id: ^id)
      |> Repo.update_all([set: [state: state, completed_at: utc_now()]], [returning: true])

    {:ok, job}
  end

  def update_state(%Job{id: id}, state) when state in ["launched"] do
    {1, [job]} =
      Job
      |> where(id: ^id)
      |> Repo.update_all([set: [state: state, completed_at: nil]], [returning: true])

    {:ok, job}
  end

  @spec retry_job(Job.t(), binary(), atom()) :: :ok
  def retry_job(%Job{} = job, formatted_error, backoff_strategy) do
    %Job{attempt: attempt, id: id, max_attempts: max_attempts, errors: errors} = job

    updates =
      if attempt >= max_attempts do
        [state: "discarded", completed_at: utc_now()]
      else
        [state: "retryable", completed_at: utc_now(), scheduled_at: next_attempt_at(attempt, backoff_strategy)]
      end

    updates =
      Keyword.put(
        updates,
        :errors,
        errors ++ [%{attempt: attempt, at: utc_now(), error: formatted_error}]
      )

    Job
    |> where(id: ^id)
    |> Repo.update_all(set: updates)
  end

  def get_job(id) do
    Repo.get(Job, id)
  end

  def count_jobs(topic) do
    Job
    |> where(topic: ^topic)
    |> Repo.aggregate(:count, :id)
  end

  def get_jobs(topic) do
    Job
    |> where(topic: ^topic)
    |> Repo.all()
  end

  def insert(job) do
    with {:ok, job} <- Repo.insert(job) do
      wake_up(job)
      notify(job)
      {:ok, job}
    end
  end

  def wake_up(%Job{topic: topic}) do
    wake_up(topic)
  end

  def wake_up(topic) do
    :topic_producer
    |> module_for()
    |> apply(:wake_up, [topic])
  end

  defp notify(%Job{topic: topic} = job) do
    JobQueue.Events.notify({:queue, :job, [topic: topic, job: job]})
  end

  def get_utilisation(topic) do
    :topic_producer
    |> module_for()
    |> apply(:pid_running, [topic])
  end

  @doc false
  def via_tuple(id) do
    {:via, Registry, {JobQueue.Registry, id}}
  end

  def via_tuple(key, topic) when key in [:supervisor, :topic_supervisor, :topic_producer] do
    key
    |> get_name_tuple(topic)
    |> via_tuple()
  end

  def process_name(:supervisor = key, name) do
    registry_lookup(key, name)
  end

  def process_name(:topic_supervisor = key, topic) do
    registry_lookup(key, topic)
  end

  def process_name(:topic_producer = key, topic) do
    registry_lookup(key, topic)
  end

  def process_name({:topic_foreman, name}, topic) do
    Module.concat([make_id(:topic_supervisor, name, topic), "Foreman"])
  end

  def registry_lookup(key, name) when key in [:supervisor, :topic_supervisor, :topic_producer] do
    Registry.lookup(JobQueue.Registry, get_name_tuple(key, name))
  end

  def make_id(:topic_supervisor, name, topic) do
    Module.concat([name, "Topic", String.capitalize(topic)])
  end

  def module_for(type) do
    case type do
      :supervisor ->
        JobQueue.Supervisor
      :topic_supervisor ->
        JobQueue.Topic.Supervisor
      :topic_producer ->
        JobQueue.Topic.Producer
    end
  end

  def get_name_tuple(key, topic) do
    {module_for(key), to_string(topic)}
  end

  def next_attempt_offset(:no_backoff, _attempt, _base_offset) do
    0
  end

  def next_attempt_offset(:linear, attempt, base_offset) do
    base_offset * attempt
  end

  def next_attempt_offset(_, attempt, base_offset) do
    trunc(:math.pow(attempt, 4) + base_offset)
  end

  # Helpers

  def next_attempt_at(base_time, attempt, backoff_strategy, base_offset \\ 15) do
    offset = next_attempt_offset(backoff_strategy, attempt, base_offset)

    NaiveDateTime.add(base_time, offset, :second)
  end

  def next_attempt_at(attempt, backoff_strategy) do
    next_attempt_at(utc_now(), attempt, backoff_strategy)
  end

  def retry_exhausted?(%{attempt: attempt}) when attempt == 0 do
    true
  end

  def retry_exhausted?(%{attempt: attempt, max_attempts: max_attempts}) when is_integer(attempt) and is_integer(max_attempts) do
    attempt == max_attempts
  end

  def retry_exhausted?(_) do
    false
  end
end
