defmodule JobQueue.Events do
  @moduledoc """
  The central event bus
  """
  use GenStage

  def start_link do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def start_link(_opts) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Sends an event and returns only after the event is dispatched.
  """
  def sync_notify(event, timeout \\ 5000) do
    GenStage.call(__MODULE__, {:notify, event}, timeout)
  end

  @doc """
  Sends an event and returns only after the event is dispatched.
  """
  def notify(event, timeout \\ 5000) do
    sync_notify(event, timeout)
  end

  @impl true
  def init(:ok) do
    {:producer, {:queue.new, 0}, dispatcher: GenStage.BroadcastDispatcher}
  end

  @impl true
  def handle_call({:notify, event}, from, {queue, demand}) do
    dispatch_events(:queue.in({from, event}, queue), demand, [])
  end

  @impl true
  def handle_cast({:notify, event}, {queue, demand}) do
    dispatch_events(:queue.in({nil, event}, queue), demand, [])
  end

  @impl true
  def handle_demand(incoming_demand, {queue, demand}) do
    dispatch_events(queue, incoming_demand + demand, [])
  end

  defp dispatch_events(queue, demand, events) do
    with d when d > 0 <- demand,
         {{:value, {from, event}}, queue} <- :queue.out(queue) do
      reply(from, :ok)
      dispatch_events(queue, demand - 1, [event | events])
    else
      _ -> {:noreply, Enum.reverse(events), {queue, demand}}
    end
  end

  defp reply(nil, _reply) do
    :ok
  end

  defp reply(from, reply) do
    GenStage.reply(from, reply)
  end
end

defmodule JobQueue.Events.Consumer do
  @moduledoc """
  Provides a simple template for event consumers
  """
  defmacro __using__(_opts \\ []) do
    quote do
      use GenStage

      require Logger

      def start_link(args) do
        GenStage.start_link(__MODULE__, args, name: __MODULE__)
      end

      def init(state) do
        Logger.info("Starting event consumer #{__MODULE__}")
        {:consumer, state, subscribe_to: [JobQueue.Events]}
      end

      def handle_events(events, from, state) do
        state = Enum.reduce(events, state, &handle_event/2)
        {:noreply, [], state}
      end

      defoverridable [start_link: 1]
    end
  end
end
