# JobQueue

## Setup

- mix deps.get
- mix ecto.reset

## Run tests

- mix test

## Try on iex

- iex -S mix
- JobQueue.get_jobs("ds")

## Job Queue

- Run a job with given concurrency, priority from a database queue
- Immediate execution on insert else poll as configured
- Broadcasting events for reporting and synchronization
- Different type of jobs - long running data-science processes, event
  based webhook workers, mapping to Java Processes

### Why another job-queue and re-invent

- there are other good alternatives available for database-based queues
  like oban, rihanna but they are tied to Postgres in particular
- specifically tying things up around Postgres NOTIFY and LISTEN
  commands
- Due to some architectural decisions, we are tied to SQLITE and hence
  have to develop custom and support reporting of errors, statistics and
accounting later which takes away the benefits of plug-and-play
packages.

### Architecture

- There are topics defined with config as `parallelism, priority,
  back_off strategy`
- It had QueueSupervisor which would start supervisors for every topic
- Every Topic Supervisor would in-turn start Topic Producer and
  Task.Supervisor for that topic
- This allows for corrupted jobs to not affect each other topics and
  continue to run

### How it works

- Topic Producer job is to poll at fixed intervals and find jobs to
  execute in the given topic
- it marks the job in `executing` state if selected for execution and
  respects concurrency
- retries on failure are supported
- with each worker, we can define max_attempts which default to 50 and
  when max_attempts are exhausted, job moves to discarded state
- at every retry, error is added to `errors` which is a `json` column
  with default to `map`, btw, it was a good feeling to know that sqlite
supports `json`
- worker is structured as a simple behavior with callbacks `perform/2,
  after_error/2, after_async/2`
- for convenience, __using__ macro used to define/override topic, other
  options like `max_attempts` with `use Topic.Worker, max_attempts: 20`
api

### How is the job completion informed, handled and concurrency
maintained

- To run workers in isolated mode to not affect the producer
- jobs are started using `Task.Supervisor.async_nolink` which on
  completion sends a message of form `{ref, result of function invoked}`
- worker jobs are bound to return either :error, {:error, msg}, {:ok,
  :pid, pid} or any other result treated as success
- In case of `:error, {:error, msg}`, job is set to retryable status and
  workers' `after_error/2` callback executed
- Interesting bit is `{:ok, :pid, pid}` which is for special cases where
  worker is essentially starting a long-running process
- In this case, pid is added to workers in progress and adds to
  concurrency
- a Process monitor is set for received pid which helps us hook into the
  message when its completed with `:DOWN` message `{:DOWN, ref,
:process, pid, reason}`
- when we get the DOWN message from pid, we make room for another job to
  run and concurrency puzzle gets all pieces sorted

### Topic Event broadcasts

- Genstage BroadcastDispatcher was used to create a event
  broadcast/subscribe system and events for topic worker started,
completed, retry added for the consumption of interesting reporting and
other dependent triggers
- Later on, it helped with the testing managing race-conditions between
  the database updates and reading them
- assertion on db's are moved next to assert_receive on different events
  as broadcasted and subscribed by a test-subscriber to send back to
test process

