defmodule JobQueue.Job do
  @moduledoc """
  A Job is an Ecto schema used for asynchronous execution.

  Job changesets are inserted into the database for asynchronous execution.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @type args :: map()
  @type errors :: [%{at: DateTime.t(), attempt: pos_integer(), error: binary()}]
  @type option ::
          {:topic, atom() | binary()}
          | {:worker, atom() | binary()}
          | {:args, args()}
          | {:max_attempts, pos_integer()}
          | {:scheduled_at, DateTime.t()}

  @type id :: Ecto.ULID.t()

  @type t :: %__MODULE__{
          id: id(),
          state: binary(),
          topic: binary(),
          worker: binary(),
          args: args(),
          errors: errors(),
          attempt: non_neg_integer(),
          max_attempts: pos_integer(),
          priority: integer(),
          inserted_at: DateTime.t(),
          attempted_at: DateTime.t(),
          scheduled_at: DateTime.t(),
          completed_at: DateTime.t()
        }

  @primary_key {:id, Ecto.ULID, autogenerate: true}

  schema "queue_jobs" do
    field :state, :string, default: "available"
    field :topic, :string
    field :priority, :integer, default: 0
    field :worker, :string
    field :args, :map
    field :errors, {:array, :map}, default: []
    field :attempt, :integer, default: 0
    field :max_attempts, :integer, default: 20

    field :generator_id, :integer

    field :attempted_at, :naive_datetime
    field :completed_at, :naive_datetime
    field :scheduled_at, :naive_datetime

    timestamps()
  end

  @permitted ~w(
    args
    priority
    attempt
    attempted_at
    completed_at
    errors
    inserted_at
    max_attempts
    topic
    state
    worker
    generator_id
    scheduled_at
  )a

  @required ~w(worker args topic)a

  @doc """
  Construct a new job changeset ready for insertion into the database.

  ## Options

    * `:max_attempts` — the maximum number of times a job can be retried if there are errors during execution
    * `:topic` — a named topic to push the job into. Jobs may be pushed into any topic, regardless
      of whether jobs are currently being processed for the topic.
    * `:scheduled_at` - a time in the future after which the job should be executed and also used as retry_at
    * `:worker` — a module to execute the job in. The module must implement the `Launcher.Worker`
      behaviour.

  ## Examples

  Insert a job with the `:default` topic:

      %{id: 1, user_id: 2}
      |> DataScience.Job.new(topic: :default, worker: Launcher.TrainWorker)
      |> JobQueue.insert()

  Generate a pre-configured job for `Launcher.TrainWorker` and push it:

      %{id: 1, user_id: 2} |> Launcher.TrainWorker.new() |> JobQueue.insert()

  """
  @spec new(args(), [option]) :: Ecto.Changeset.t()
  def new(args, opts \\ []) when is_map(args) and is_list(opts) do
    params =
      opts
      |> Keyword.put(:args, args)
      |> Map.new()
      |> coerce_field(:topic)
      |> coerce_field(:worker)

    %__MODULE__{}
    |> cast(params, @permitted)
    |> validate_required(@required)
    |> validate_number(:max_attempts, greater_than: 0, less_than: 50)
  end

  defp coerce_field(params, field) do
    case Map.get(params, field) do
      value when is_atom(value) and not is_nil(value) ->
        update_in(params, [field], &to_clean_string/1)

      _ ->
        params
    end
  end

  defp to_clean_string(atom) do
    atom
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
  end
end
