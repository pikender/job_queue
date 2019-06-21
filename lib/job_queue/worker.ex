defmodule JobQueue.Worker do
  @moduledoc """
  Defines a behavior and macro to guide the creation of worker modules.

  Worker modules do the work of processing a job. At a minimum they must define a `perform/1`
  function, which will be called with `job` and `args` map.

  ## Defining Workers

  Define a worker to process jobs in the `events` topic:

      defmodule DataScience.TrainWorker do
        use JobQueue.Worker, topic: "events", max_attempts: 10

        @impl JobQueue.Worker
        def perform(job, args) do
          IO.inspect(job, args)
        end
      end

  For DataScience jobs, Launcher.Supervisor is used to launch jobs and this module
  merely works as a wrapper to get arguments to start Launcher.
  If `perform/1` raises an exception the job is considered failed and retried.

  ## Enqueuing Jobs

  All workers implement a `new/2` function that converts an args map into a job changeset
  suitable for inserting into the database for later execution:

      %{in_the: "business", of_doing: "business"}
      |> DataScience.TrainWorker.new()
      |> JobQueue.insert()

  The worker's defaults may be overridden by passing options:

      %{vote_for: "none of the above"}
      |> DataScience.TrainWorker.new(topic: "special", max_attempts: 5)
      |> JobQueue.insert()

  See `DataScience.Job` for all available options.
  """

  alias JobQueue.Job

  @doc """
  Return the name of the topic the worker writes to.
  """
  @callback topic() :: JobQueue.topic_name()

  @doc """
  Return the worker's default priority setting.
  """
  @callback priority() :: integer()

  @doc """
  Build a job changeset for this worker with optional overrides.

  See `DataScience.Job.new/2` for the available options.
  """
  @callback new(args :: Job.args(), opts :: [Job.option()]) :: Ecto.Changeset.t()

  @doc """
  The `perform/1` function is called when the job is executed.

  The function is passed a job and job's args (which is always a map with string keys).

  For DataScience Jobs, the return value is used as `args` to Launcher in case the
  function executes without raising an exception. For other types, it might be ignored.
  If the job raises an exception it is a failure and the job may be
  scheduled for a retry.
  """
  @callback perform(job :: Job, args :: map()) :: term()

  @callback after_error(job :: Job, reason :: term(), args :: map()) :: term()
  @callback after_async(job :: Job, pid :: pid()) :: term()

  @callback datascience_process_type() :: :generate | :train | :unknown

  defmodule AbsentTopic do
    defexception [:message]

    @impl true
    def exception(_value) do
      msg = "missing topic, expected `use JobQueue.Worker, topic: :topic_name`"
      %AbsentTopic{message: msg}
    end
  end

  @doc false
  defmacro __using__(opts) do
    quote location: :keep do
      alias JobQueue.{Job, Worker}

      @behaviour Worker

      if unquote(opts) |> Keyword.get(:topic, nil) |> is_nil() do
        raise AbsentTopic
      end

      @opts unquote(opts)
            |> Keyword.take([:topic, :max_attempts, :priority])
            |> Keyword.put_new(:priority, 0)
            |> Keyword.put(:worker, to_string(__MODULE__))

      @topic Keyword.fetch!(@opts, :topic)
      @priority Keyword.fetch!(@opts, :priority)

      @impl Worker
      def topic do
        @topic
      end

      @impl Worker
      def priority do
        @priority
      end

      @impl Worker
      def new(args, opts \\ []) when is_map(args) do
        Job.new(args, Keyword.merge(@opts, opts))
      end

      @impl Worker
      def perform(job, args) when is_map(args) do
        :ok
      end

      @impl Worker
      def after_error(job, reason, arg) do
        :ok
      end

      @impl Worker
      def after_async(job, pid) do
        :ok
      end

      @impl Worker
      def datascience_process_type() do
        :unknown
      end

      defoverridable Worker
    end
  end
end
