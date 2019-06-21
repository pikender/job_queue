defmodule JobQueue.Topic.Producer do
  @moduledoc """
  Producer is responsible for fetching available jobs from database,
  execute them and observe for success/failures. It marks the jobs as
  completed or retryable based on output running the worker module.

  **A quirk in case of worker returning {:ok, :pid, pid}**

  - The job state is changed to `launched` and not `completed` or `retryable`
  - `launched` state is an intermediate state between `executing` to `completed|retryable`
  - As whether launched pid completed successfully or not is more of an application logic and opaque to Producer here
  - Producer completes the feedback loop by invoking `after_async/2` callback defined in worker
  - It's exepected that `after_async/2` callback will have enough info based on application logic to mark the job as `completed` or `retryable`
  - `application logic` example could be inspecting a db record generated|updated as the result of pid execution or some file generation
  - `after_async/2` would return `:discard|{:error, reason}` to mark jobs as `discarded|retryable` respectively, any other value marks job as `completed`

  ```
  defmodule AsyncWorker do
    use JobQueue.Worker, topic: :async

    def perform(job, args) do
      case SomeGenServer.start_link() do
        {:ok, pid} ->
          {:ok, :pid, pid}
        _ ->
          :error
      end
    end

    def after_async(job, pid) do
      if File.exists?("job-{job.id}.json") or Repo.get(Table, job.id) do
        :ok
      else
        {:error, "another try please"}
      end
    end
  end
  ```
  """

  use GenServer

  require Logger

  alias JobQueue, as: Queue

  @type option ::
          {:name, module()}
          | {:conf, Queue.Config.t()}
          | {:foreman, GenServer.name()}
          | {:limit, pos_integer()}
          | {:topic, binary()}
          | {:backoff_strategy, atom()}

  defmodule State do
    @moduledoc false

    @enforce_keys [:conf, :foreman, :limit, :topic, :backoff_strategy]
    defstruct [:conf, :foreman, :limit, :topic, :backoff_strategy, running: %{}]
  end

  defdelegate to_module(worker), to: JobQueue.Util

  @doc false
  def pid_running(topic) do
    :topic_producer
    |> Queue.via_tuple(topic)
    |> GenServer.call({:pid_running, topic})
  end

  @doc """
  Producer checks the topic for jobs immediately
  """
  def wake_up(topic) do
    :topic_producer
    |> Queue.via_tuple(topic)
    |> GenServer.cast({:wake_up, topic})
  end

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    topic = Keyword.get(opts, :topic)

    GenServer.start_link(__MODULE__, opts, name: Queue.via_tuple(:topic_producer, topic))
  end

  @impl GenServer
  def init(opts) do
    {:ok, struct!(State, opts), {:continue, :start}}
  end

  @impl GenServer
  def handle_continue(:start, state) do
    state
    |> rescue_orphans()
    |> start_interval()
    |> dispatch()
  end

  @impl GenServer
  def handle_cast({:wake_up, topic}, %State{topic: topic} = state) do
    {_, new_state} = dispatch(state)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    state
    |> deschedule()
    |> dispatch()
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    %State{topic: topic, running: running, backoff_strategy: backoff_strategy} = state

    {{job, ref_or_pid}, now_running} = Map.pop(running, ref)

    if is_pid(ref_or_pid) do
      Process.demonitor(ref)

      to_module(job.worker)
      |> apply(:after_async, [job, pid])
      |> handle_after_async(job, backoff_strategy)

      launch_events(:worker_pid_completed, [topic: topic, job_id: job.id, pid: pid])
    else
      job_raised(job, reason, backoff_strategy)

      launch_events(:worker_crashed, [topic: topic, job_id: job.id, pid: pid])
    end

    dispatch(%{state | running: now_running})
  end

  def handle_info({ref, result}, state) do
    %State{topic: topic, running: running, backoff_strategy: backoff_strategy} = state
    # Flush guarantees that any DOWN messages will be received before
    # demonitoring. This is probably unnecessary but it can't hurt to be sure.
    Process.demonitor(ref, [:flush])

    {{job, _ref_or_pid}, now_running} = Map.pop(running, ref)

    change_now_running =
      case result do
        {:error, _} ->
          job_failure(job, result, backoff_strategy)

          now_running

        :error ->
          job_failure(job, result, backoff_strategy)

          now_running

        {:ok, :pid, pid} ->
          Queue.update_state(job, "launched")

          Map.put(now_running, Process.monitor(pid), {job, pid})

        _ ->
          Queue.complete_job(job)

          now_running
      end

    launch_events(:worker_completed, [topic: topic, job_id: job.id, result: result])

    state = %{state | running: change_now_running}

    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.info(fn -> "Unhandled _info message received #{inspect message}" end)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:pid_running, _topic}, _from, state) do
    %State{running: running} = state

    utilisation =
      running
      |> Map.values()
      |> Enum.filter(fn {_job, ref_or_pid} ->
        is_pid(ref_or_pid)
      end)
      |> case do
        [] ->
          %{generator_id: nil, utilisation: 0, type: nil}
        [{job, _}] ->
          %{generator_id: job.generator_id, utilisation: 1, type: datascience_process_type(job)}
        records ->
          Logger.info(fn -> "Expected only one record but got #{inspect records}" end)
          [{job, _}|_] = records
          %{generator_id: job.generator_id, utilisation: 1, type: datascience_process_type(job)}
      end

    {:reply, utilisation, state}
  end

  # Start Handlers

  defp rescue_orphans(%State{topic: topic} = state) do
    Queue.rescue_orphaned_jobs(topic)

    state
  end

  defp start_interval(%State{conf: conf} = state) do
    {:ok, _ref} = :timer.send_interval(conf.poll_interval, :poll)

    state
  end

  # Dispatching

  defp deschedule(%State{topic: topic} = state) do
    Queue.stage_scheduled_jobs(topic)

    state
  end

  defp dispatch(%State{limit: limit, running: running} = state) when map_size(running) >= limit do
    {:noreply, state}
  end

  defp dispatch(%State{foreman: foreman} = state) do
    %State{topic: topic, limit: limit, running: running} = state

    demand = limit - map_size(running)

    jobs = Queue.get_available_jobs(topic, demand)

    started_jobs =
      for job <- jobs, into: %{} do
        Logger.info("topic: #{topic}; start job #{job.id}")
        task = spawn_supervised_task(foreman, job)
        {task.ref, {job, task.ref}}
      end

    {:noreply, %{state | running: Map.merge(running, started_jobs)}}
  end

  defp job_raised(job, reason, backoff_strategy) do
    Queue.retry_job(job, Exception.format_exit(reason), backoff_strategy)
  end

  defp job_failure(job, reason, backoff_strategy) do
    %{worker: worker, args: args} = job

    Queue.retry_job(job, "Job Failed\n#{inspect(reason, limit: :infinity)}", backoff_strategy)

    apply(to_module(worker), :after_error, [job, reason, args])
  end

  defp spawn_supervised_task(foreman, job) do
    %{worker: worker, args: args} = job

    Task.Supervisor.async_nolink(foreman, fn ->
      result =
        worker
        |> to_module()
        |> apply(:perform, [job, args])

      result
    end)
  end

  defp handle_after_async(result, job, backoff_strategy) do
    case result do
      {:error, reason} ->
        Queue.retry_job(job, reason, backoff_strategy)

      :discard ->
        Queue.discard_job(job)

      _ ->
        Queue.complete_job(job)
    end
  end

  defp launch_events(type, data) do
    JobQueue.Events.notify({:queue, type, data})
  end

  defp datascience_process_type(%Queue.Job{worker: _worker} = job) do
    job
    |> find_datascience_process_type()
    |> handle_datascience_process_type()
  end

  defp find_datascience_process_type(%Queue.Job{worker: worker}) do
    worker
    |> to_module()
    |> apply(:datascience_process_type, [])
  rescue
    error -> error
  end

  defp handle_datascience_process_type(:generate), do: "g"
  defp handle_datascience_process_type(:train), do: "t"
  defp handle_datascience_process_type(_), do: nil
end
