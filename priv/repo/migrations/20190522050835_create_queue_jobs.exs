defmodule JobQueue.Repo.Migrations.CreateQueueJobs do
  use Ecto.Migration

  defmacro now do
    quote do
      fragment("(STRFTIME('%Y-%m-%d %H:%M:%f000', 'NOW'))")
    end
  end

  def change do
    create table(:queue_jobs, primary_key: false) do
      add :id, :binary_id, null: false, primary_key: true
      add :state, :string, null: false, default: "available"
      add :topic, :string, null: false
      add :worker, :text, null: false
      add :args, :map, null: false
      add :errors, {:array, :map}, null: false, default: "[]"
      add :attempt, :integer, null: false, default: 0
      add :max_attempts, :integer, null: false, default: 20
      add :priority, :integer, null: false, default: 0

      add :generator_id, :bigint

      # Matching default for timestamps type
      add :attempted_at, :naive_datetime
      add :completed_at, :naive_datetime
      add :scheduled_at, :naive_datetime, default: now()

      timestamps()
    end

    create index(:queue_jobs, [:topic])
    create index(:queue_jobs, [:state])
    create index(:queue_jobs, [:scheduled_at])
  end
end
