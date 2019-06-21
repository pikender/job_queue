defmodule JobQueue.Test.EventSubscriber do
  @moduledoc false
  use JobQueue.Events.Consumer

  def start_link(parent \\ self()) do
    GenStage.start_link(__MODULE__, parent)
  end

  def handle_event(event, parent) do
    send(parent, {JobQueue.Events, event})
    parent
  end
end
