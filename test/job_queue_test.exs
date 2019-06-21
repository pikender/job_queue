defmodule JobQueue.QueueTest do
  use JobQueue.DataCase

  alias JobQueue.Mocks.{
    ReturnPidWorker,
    AfterAsyncPidWorker,
    DeadPidWorker,
    ErrorWorker,
    SourceOfTruth,
    InformAgentPid
  }

  @topic_option [parallelism: 1, backoff_strategy: :exponential]
  @wh_topic_option [parallelism: 1, backoff_strategy: :linear]
  @no_topic_option [parallelism: 1, backoff_strategy: :no_backoff]

  defmodule HighPriorityWorker do
    use JobQueue.Worker, topic: "urgent", priority: 20
  end

  describe "HighPriorityWorker" do
    test "returns the default configured priority" do
      assert HighPriorityWorker.priority() == 20
    end

    test "creates new jobs with the configured priority" do
      job =
        %{}
        |> HighPriorityWorker.new()
        |> Ecto.Changeset.apply_changes()
      assert job.priority == 20
    end

    test "allows for overriding the default priority" do
      job =
        %{}
        |> HighPriorityWorker.new(priority: 0)
        |> Ecto.Changeset.apply_changes()
      assert job.priority == 0
    end
  end

  describe "ReturnPidWorker" do
    setup do
      parent_name = :return_pid_test

      # Sending message to self() but passed as name
      Process.register(self(), parent_name)

      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _queue_supervisor} = JobQueue.start_link(topics: [ds: @topic_option], poll_interval: 50)

      {:ok, %{parent_name: parent_name}}
    end

    test "returns a valid topic name" do
      assert ReturnPidWorker.topic() == "ds"
    end

    test "returns the configured priority" do
      assert ReturnPidWorker.priority() == 0
    end

    test "doesn't pick till perform/2 process alive", %{parent_name: parent_name} do
      args = %{
        parent: parent_name,
        process_name: :return_pid_worker1
      }

      {:ok, j} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      assert j.args == args

      job_id = j.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: {:ok, :pid, launch_pid}]}}, 500

      nj = JobQueue.get_job(j.id)

      assert nj.state == "launched"
      assert nj.errors == []

      assert Process.alive?(launch_pid)

      {:ok, j1} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      nj1 = JobQueue.get_job(j1.id)

      assert nj1.state == "available"
      assert is_nil(nj1.attempted_at)

      # Finish Process
      :ok = JobQueue.Mocks.LaunchPid.stop(args.process_name)

      refute Process.alive?(launch_pid)

      job_id = j1.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: {:ok, :pid, _launch_pid}]}}, 500

      nj2 = JobQueue.get_job(j1.id)

      assert nj2.state == "launched"
    end

    test "not implementing after_async/2 marks the job as completed", %{parent_name: parent_name} do
      args = %{
        parent: parent_name,
        process_name: :return_pid_worker2
      }

      {:ok, j} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      assert j.args == args

      job_id = j.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: {:ok, :pid, launch_pid}]}}, 500

      nj = JobQueue.get_job(j.id)

      assert nj.state == "launched"
      assert nj.errors == []

      assert Process.alive?(launch_pid)

      # Finish Process
      :ok = JobQueue.Mocks.LaunchPid.stop(args.process_name)

      refute Process.alive?(launch_pid)

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      nj = JobQueue.get_job(j.id)

      assert nj.state == "completed"
    end

    test "process is datascience generate or train", %{parent_name: parent_name} do
      args = %{
        parent: parent_name,
        process_name: :return_pid_worker4
      }

      {:ok, j} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      assert j.args == args

      job_id = j.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: {:ok, :pid, launch_pid}]}}, 500

      nj = JobQueue.get_job(j.id)

      assert nj.state == "launched"
      assert nj.errors == []

      assert Process.alive?(launch_pid)

      assert %{generator_id: nil, type: "g", utilisation: 1} == JobQueue.get_utilisation("ds")

      # Finish Process
      :ok = JobQueue.Mocks.LaunchPid.stop(args.process_name)

      refute Process.alive?(launch_pid)

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      assert %{generator_id: nil, type: nil, utilisation: 0} == JobQueue.get_utilisation("ds")

      nj = JobQueue.get_job(j.id)

      assert nj.state == "completed"
    end

    def process_alive(name) do
      pid = Process.whereis(name)

      if pid do
        Process.alive?(pid)
      else
        false
      end
    end
  end

  describe "Support Modules" do
    test "agent mock" do
      name = :sot
      key = "q"
      value = "a"
      {:ok, _agent} = SourceOfTruth.start_link(name)
      assert SourceOfTruth.lookup(name, key) == nil
      assert SourceOfTruth.insert(name, key, value) == :ok
      assert SourceOfTruth.lookup(name, key) == value
      assert SourceOfTruth.delete(name, key) == :ok
      assert SourceOfTruth.lookup(name, key) == nil
    end

    test "pid informing success/failure which can be inspected later" do
      agent_name = :sot
      {:ok, _agent} = SourceOfTruth.start_link(agent_name)
      {:ok, p} = InformAgentPid.start_link(agent_name)
      assert SourceOfTruth.lookup(agent_name, p) |> is_nil()
      InformAgentPid.mark_success(:started)
      assert SourceOfTruth.lookup(agent_name, p) == {:success, :started}
      InformAgentPid.stop()
    end
  end

  describe "AfterAsyncPidWorker" do
    setup do
      agent_name = :sot

      {:ok, _agent} = SourceOfTruth.start_link(agent_name)

      args = %{
        user_id: 1,
        input_id: 2,
        generator_id: 3,
        agent_name: agent_name
      }

      {:ok, job} =
        args
        |> AfterAsyncPidWorker.new()
        |> JobQueue.insert()

      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _queue_supervisor} = JobQueue.start_link(topics: [ds: @topic_option], poll_interval: 50)

      {:ok, %{job: job, args: args}}
    end

    test "returning :discard in after_async/2 marks the job as discarded", %{job: job, args: args} do
      assert job.args == args

      job_id = job.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: _]}}, 500

      assert JobQueue.Mocks.InformAgentPid |> process_alive()

      nj = JobQueue.get_job(job.id)

      assert nj.state == "launched"
      assert nj.errors == []

      # Send nothing to agent

      # Finish Process
      InformAgentPid.stop()

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      refute JobQueue.Mocks.InformAgentPid |> process_alive()

      nj2 = JobQueue.get_job(job.id)

      assert nj2.state == "discarded"
    end

    test "returning :ok in after_async/2 marks the job as completed", %{job: job, args: args} do
      assert job.args == args

      job_id = job.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: _]}}, 500

      assert JobQueue.Mocks.InformAgentPid |> process_alive()

      nj = JobQueue.get_job(job.id)

      assert nj.state == "launched"
      assert nj.errors == []

      # Send success to process which would be used
      # in after_async/2 callback to mark complete/failure job state
      assert {:success, :model_generated} == InformAgentPid.mark_success(:model_generated)

      # Finish Process
      InformAgentPid.stop()

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      refute JobQueue.Mocks.InformAgentPid |> process_alive()

      nj2 = JobQueue.get_job(job.id)

      assert nj2.state == "completed"
    end

    test "returning {:error, reason} in after_async/2 marks the job as retryable", %{job: job, args: args} do
      assert job.args == args

      job_id = job.id

      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: _]}}, 500

      assert JobQueue.Mocks.InformAgentPid |> process_alive()

      nj = JobQueue.get_job(job.id)

      assert nj.state == "launched"
      assert nj.errors == []

      # Send success to process which would be used
      # in after_async/2 callback to mark complete/failure job state
      assert {:failure, :model_generated} == InformAgentPid.mark_failure(:model_generated)

      # Finish Process
      InformAgentPid.stop()

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      refute JobQueue.Mocks.InformAgentPid |> process_alive()

      nj2 = JobQueue.get_job(job.id)

      assert nj2.state == "retryable"
      [%{"attempt" => error_attempt, "error" => error_msg}] = nj2.errors

      assert error_attempt == 1
      assert Regex.match?(~r/pid_failed/, error_msg)
    end
  end

  describe "DeadPidWorker" do
    setup do
      args = %{
        user_id: 1,
        input_id: 2,
        generator_id: 3
      }

      {:ok, j} =
        args
        |> DeadPidWorker.new()
        |> JobQueue.insert()

      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _queue_supervisor} = JobQueue.start_link(topics: [ds: @topic_option], poll_interval: 1)

      {:ok, %{job: j, args: args}}
    end

    test "should invoke after_async/2 callback", %{job: job, args: args} do
      assert job.args == args

      job_id = job.id

      assert_receive {JobQueue.Events, {:queue, :worker_pid_completed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      nj = JobQueue.get_job(job.id)

      assert nj.state == "discarded"
      assert nj.errors == []
    end
  end

  describe "after_error/3 callback" do
    setup do
      args = %{user_id: 1}

      {:ok, j} =
        args
        |> ErrorWorker.new()
        |> JobQueue.insert()

      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _queue_supervisor} = JobQueue.start_link(topics: [ds: @topic_option], poll_interval: 10)

      {:ok, %{job: j, args: args}}
    end

    test "worker crashes", %{job: job, args: args} do
      assert job.args == args

      job_id = job.id
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: "ds", job_id: ^job_id, pid: _]}}, 500

      nj = JobQueue.get_job(job.id)

      assert nj.state == "retryable"
      [%{"attempt" => error_attempt, "error" => error_msg}] = nj.errors

      assert error_attempt == 1
      assert Regex.match?(~r/bad argument in arithmetic expression/, error_msg)
    end
  end

  describe "insert/1" do
    test "puts the record in database" do
      args = %{user_id: 1}

      {status, record} =
        args
        |> ErrorWorker.new()
        |> JobQueue.insert()

      assert :ok == status
      assert record.args == args

      assert JobQueue.get_job(record.id)
    end
  end

  describe "wake_up/1" do
    setup do
      parent_name = :return_pid_test

      # Sending message to self() but passed as name
      Process.register(self(), parent_name)

      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _pid} = JobQueue.start_link(topics: [ds: @topic_option], poll_interval: 10_000)

      {:ok, %{parent_name: parent_name, topic: "ds"}}
    end

    # Ideally this can be tested with allowing poll to be stopped and than making the assertion
    test "starts the job immediately", %{parent_name: parent_name, topic: topic} do
      args = %{
        parent: parent_name,
        process_name: :return_pid_worker3
      }

      {:ok, j} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      assert j.args == args

      job_id = j.id

      refute_received {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: _]}}

      :ok = JobQueue.wake_up(topic)
      assert_receive {JobQueue.Events, {:queue, :worker_completed, [topic: "ds", job_id: ^job_id, result: _]}}, 100
    end
  end

  describe "rescue_orphaned_jobs/1" do
    setup do
      parent_name = :return_pid_test
      {:ok, parent_name: parent_name}
    end

    test "moves `executing` to `available`", cxt do
      args = %{
        parent: cxt.parent_name
      }

      assert {:ok, job} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      assert [executing_job] = JobQueue.get_available_jobs(job.topic, 1)
      assert executing_job.id == job.id

      assert {1, [available_job]} = JobQueue.rescue_orphaned_jobs(job.topic)
      assert available_job.id == job.id
      assert available_job.state == "available"
    end

    test "moves `launched` to `available`", cxt do
      args = %{
        parent: cxt.parent_name
      }

      assert {:ok, job} =
        args
        |> ReturnPidWorker.new()
        |> JobQueue.insert()

      JobQueue.update_state(job, "launched")

      assert {1, [available_job]} = JobQueue.rescue_orphaned_jobs(job.topic)

      assert available_job.id == job.id
      assert available_job.state == "available"
    end
  end

  describe "backoff" do
    setup do
      {:ok, _subscriber} = JobQueue.Test.EventSubscriber.start_link()

      {:ok, _pid} = JobQueue.start_link(
                                      topics: [ds: @topic_option, wh: @wh_topic_option, no: @no_topic_option],
                                      poll_interval: 10_000)

      {:ok, %{linear_backoff_topic: "wh", exponential_backoff_topic: "ds", no_backoff_topic: "no"}}
    end

    test "exponential_backoff at queue level", %{exponential_backoff_topic: exp_backoff_topic} do
      args = %{user_id: 1}

      {:ok, j} =
        args
        |> ErrorWorker.new()
        |> JobQueue.insert()

      assert j.args == args
      assert j.topic == exp_backoff_topic

      # Due to sqlite `returning` gotcha, scheduled_at coming as nil though set in DB
      # Loading record from DB
      nj = JobQueue.get_job(j.id)
      %JobQueue.Job{id: job_id} = nj

      :ok = JobQueue.wake_up(exp_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: "ds", job_id: ^job_id, pid: _]}}, 100

      nj1 = JobQueue.get_job(j.id)

      assert_in_delta NaiveDateTime.diff(nj1.scheduled_at, NaiveDateTime.utc_now()),
                      JobQueue.next_attempt_offset(:exponential, nj.attempt, 15),
                      1

      # Mutate scheduled_at to kick-off second run
      JobQueue.Job
      |> where(id: ^job_id)
      |> JobQueue.Repo.update_all(set: [state: "available", scheduled_at: NaiveDateTime.add(NaiveDateTime.utc_now(), -60)])

      njj1 = JobQueue.get_job(j.id)
      assert NaiveDateTime.diff(njj1.scheduled_at, NaiveDateTime.utc_now()) <= 0

      :ok = JobQueue.wake_up(exp_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: "ds", job_id: ^job_id, pid: _]}}, 100

      nj2 = JobQueue.get_job(j.id)

      assert NaiveDateTime.diff(nj2.scheduled_at, NaiveDateTime.utc_now()) > JobQueue.next_attempt_offset(:exponential, nj1.attempt, 15)
    end

    test "linear_backoff at queue level", %{linear_backoff_topic: linear_backoff_topic} do
      args = %{user_id: 1}

      {:ok, j} =
        args
        |> ErrorWorker.new([topic: linear_backoff_topic])
        |> JobQueue.insert()

      assert j.args == args
      assert j.topic == linear_backoff_topic

      # Due to sqlite `returning` gotcha, scheduled_at coming as nil though set in DB
      # Loading record from DB
      nj = JobQueue.get_job(j.id)
      %JobQueue.Job{id: job_id} = nj

      :ok = JobQueue.wake_up(linear_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: "wh", job_id: ^job_id, pid: _]}}, 100

      nj1 = JobQueue.get_job(j.id)

      assert_in_delta NaiveDateTime.diff(nj1.scheduled_at, NaiveDateTime.utc_now()),
                      JobQueue.next_attempt_offset(:linear, nj.attempt, 15),
                      16

      # Mutate scheduled_at to kick-off second run
      JobQueue.Job
      |> where(id: ^job_id)
      |> JobQueue.Repo.update_all(set: [state: "available", scheduled_at: NaiveDateTime.add(NaiveDateTime.utc_now(), -60)])

      njj1 = JobQueue.get_job(j.id)
      assert NaiveDateTime.diff(njj1.scheduled_at, NaiveDateTime.utc_now()) <= 0

      :ok = JobQueue.wake_up(linear_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: "wh", job_id: ^job_id, pid: _]}}, 100

      nj2 = JobQueue.get_job(j.id)

      assert_in_delta NaiveDateTime.diff(nj2.scheduled_at, NaiveDateTime.utc_now()),
                      JobQueue.next_attempt_offset(:linear, nj1.attempt, 15),
                      16
    end

    test "no_backoff at queue level", %{no_backoff_topic: no_backoff_topic} do
      args = %{user_id: 1}

      {:ok, j} =
        args
        |> ErrorWorker.new([topic: no_backoff_topic])
        |> JobQueue.insert()

      assert j.args == args
      assert j.topic == no_backoff_topic

      # Due to sqlite `returning` gotcha, scheduled_at coming as nil though set in DB
      # Loading record from DB
      nj = JobQueue.get_job(j.id)
      %JobQueue.Job{id: job_id} = nj

      :ok = JobQueue.wake_up(no_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: ^no_backoff_topic, job_id: ^job_id, pid: _]}}, 100

      nj1 = JobQueue.get_job(j.id)

      assert_in_delta NaiveDateTime.diff(nj1.scheduled_at, NaiveDateTime.utc_now()),
                      JobQueue.next_attempt_offset(:no_backoff, nj.attempt, 15),
                      1

      # Make the job available so that wake_up picks it
      # scheduled_at set is not required as no_backoff
      JobQueue.Job
      |> where(id: ^job_id)
      |> JobQueue.Repo.update_all(set: [state: "available"])

      njj1 = JobQueue.get_job(j.id)
      assert NaiveDateTime.diff(njj1.scheduled_at, NaiveDateTime.utc_now()) <= 0

      :ok = JobQueue.wake_up(no_backoff_topic)
      assert_receive {JobQueue.Events, {:queue, :worker_crashed, [topic: ^no_backoff_topic, job_id: ^job_id, pid: _]}}, 100

      nj2 = JobQueue.get_job(j.id)

      assert_in_delta NaiveDateTime.diff(nj2.scheduled_at, NaiveDateTime.utc_now()),
                      JobQueue.next_attempt_offset(:no_backoff, nj1.attempt, 15),
                      1
    end
  end

  describe "get_available_jobs/2" do
    setup do
      parent_name = :return_pid_test
      {:ok, parent_name: parent_name}
    end

    test "respects the job priority setting", cxt do
      args = %{
        parent: cxt.parent_name
      }

      # start with a base time < now so that all the jobs are
      # available (the jobs are filtered by scheduled_at <= now)
      now = NaiveDateTime.add(NaiveDateTime.utc_now(), -7_200)

      assert {:ok, job_priority_low} =
        args
        |> ReturnPidWorker.new(priority: 0, scheduled_at: now)
        |> JobQueue.insert()

      assert {:ok, job_priority_high} =
        args
        |> ReturnPidWorker.new(priority: 1, scheduled_at: NaiveDateTime.add(now, 3_600))
        |> JobQueue.insert()

      JobQueue.get_jobs(job_priority_low.topic)

      assert [executing_job] = JobQueue.get_available_jobs(ReturnPidWorker.topic(), 1)

      assert executing_job.id == job_priority_high.id
    end
  end
end
