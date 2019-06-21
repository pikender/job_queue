defmodule JobQueue.Mocks do
  defmodule ReturnPidWorker do
    use JobQueue.Worker, topic: "ds", max_attempts: 5

    def perform(job, %{"parent" => parent, "process_name" => process_name}) do
      {:ok, pid} = JobQueue.Mocks.LaunchPid.start_link(make_process_name(parent), job, make_process_name(process_name))
      {:ok, :pid, pid}
    end

    def datascience_process_type() do
      :generate
    end

    defp make_process_name(str) do
      String.to_existing_atom(str)
    end
  end

  defmodule DeadPidWorker do
    use JobQueue.Worker, topic: "ds", max_attempts: 5

    # fake pid numbers have no significance
    # hopefully don't clash with real pid
    def perform(_job, _args) do
      pid = :c.pid(0,250,22)
      assert_dead_pid(pid)
      {:ok, :pid, pid}
    end

    def after_async(_job, _pid) do
      :discard
    end

    defp assert_dead_pid(pid) do
      if Process.alive?(pid) do
        raise "must be a dead pid"
      end
    end
  end

  defmodule AfterAsyncPidWorker do
    use JobQueue.Worker, topic: "ds", max_attempts: 5

    # Simple conversion to atom to help with name resolution
    # than using Registry functions
    def perform(_job, %{"agent_name" => agent_name}) do
      {:ok, pid} =
        agent_name
        |> convert_str_to_atom_for_name()
        |> JobQueue.Mocks.InformAgentPid.start_link()

      {:ok, :pid, pid}
    end

    # Inspect SourceOfTruth value to mark success/failure
    def after_async(%{args: %{"agent_name" => agent_name}}, ppid) do
      inspect_artifact_to_mark_job_status =
        agent_name
        |> convert_str_to_atom_for_name()
        |> JobQueue.Mocks.SourceOfTruth.lookup(ppid)

      case inspect_artifact_to_mark_job_status do
        {:success, _} ->
          :ok

        {:failure, _} ->
          {:error, :pid_failed}

        _ ->
          :discard
      end
    end

    def convert_str_to_atom_for_name(str) do
      String.to_existing_atom(str)
    end
  end

  defmodule ErrorWorker do
    use JobQueue.Worker, topic: "ds", max_attempts: 5

    def perform(_job, _args) do
      _ = 1/0
    end

    def after_error(job, reason, _args) do
      IO.inspect("#{job.id} failed with #{reason}")
    end
  end

  defmodule SourceOfTruth do
    use Agent

    def start_link(name) do
      Agent.start_link(fn -> %{} end, name: name)
    end

    def inspect(agent) do
      Agent.get(agent, & &1)
    end

    def lookup(agent, key) do
      Agent.get(agent, &Map.get(&1, key))
    end

    def insert(agent, key, value) do
      Agent.update(agent, &Map.put(&1, key, value))
    end

    def delete(agent, key) do
      Agent.update(agent, &Map.delete(&1, key))
    end
  end

  defmodule LaunchPid do
    use GenServer

    def stop(name) do
      GenServer.stop(name)
    end

    def start_link(parent, job, name) do
      GenServer.start_link(__MODULE__, {parent, job}, name: name)
    end

    def init({parent, job}) do
      {:ok, %{parent: parent, job: job}, {:continue, :inform_parent}}
    end

    def handle_continue(:inform_parent, %{parent: parent, job: job} = state) do
      send parent, {:started, :launch_pid, self(), job.id}
      {:noreply, state}
    end
  end

  defmodule InformAgentPid do
    use GenServer

    def stop() do
      GenServer.stop(__MODULE__)
    end

    def mark_success(val) do
      GenServer.call(__MODULE__, {:success, val})
    end

    def mark_failure(val) do
      GenServer.call(__MODULE__, {:failure, val})
    end

    def start_link(agent) do
      GenServer.start_link(__MODULE__, [agent], name: __MODULE__)
    end

    def init([agent]) do
      {:ok, %{agent: agent, value: nil}}
    end

    def handle_call(val, _from, %{agent: agent} = state) do
      SourceOfTruth.insert(agent, self(), val)
      {:reply, val, %{state | value: val}}
    end
  end
end
