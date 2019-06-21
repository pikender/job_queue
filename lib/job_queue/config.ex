defmodule JobQueue.Config do
  defstruct name: Generator,
            poll_interval: :timer.seconds(1),
            topics: []

  @type topic_option :: {:parallelism, pos_integer()} | {:backoff_strategy, module()}

  @type t :: %__MODULE__{
          name: module(),
          poll_interval: pos_integer(),
          topics: [{atom(), [topic_option()]}],
        }

  @spec new(Keyword.t()) :: t()
  def new(opts) when is_list(opts) do
    Enum.each(opts, &validate_opt!/1)

    struct!(__MODULE__, opts)
  end

  def topic_parallelism(topic_otion) do
    Keyword.get(topic_otion, :parallelism)
  end

  def topic_backoff_strategy(topic_otion) do
    Keyword.get(topic_otion, :backoff_strategy)
  end

  defp validate_opt!({:poll_interval, interval}) do
    unless is_integer(interval) and interval > 0 do
      raise ArgumentError, "expected :poll_interval to be a positive integer"
    end
  end

  defp validate_opt!({:topics, topics}) do
    unless Keyword.keyword?(topics) and Enum.all?(topics, &valid_topic?/1) do
      raise ArgumentError,
            "expected :topics to be a keyword list of {atom, topic_option} pairs where topic_option allows parallelism and backoff_strategy"
    end
  end

  defp validate_opt!(_opt), do: :ok

  defp valid_topic?({name, topic_option}) do
    valid_parallelism?({name, topic_option}) &&
    valid_backoff_strategy?({name, topic_option})
  end

  defp valid_parallelism?({name, topic_option}) do
    parallelism = Keyword.get(topic_option, :parallelism)
    if valid_parallelism?(parallelism) do
      true
    else
      raise ArgumentError, "expected :parallelism for topic #{name} to be an integer but got #{parallelism}"
    end
  end

  defp valid_parallelism?(size), do: is_integer(size) and size > 0

  @backoff_strategies [:no_backoff, :linear, :exponential]
  defp valid_backoff_strategy?({name, topic_option}) do
    backoff_strategy = Keyword.get(topic_option, :backoff_strategy)
    if valid_backoff_strategy?(backoff_strategy) do
      true
    else
      raise ArgumentError, "expected :backoff_strategy for topic #{name} to be one of [:linear, :exponential] but got #{backoff_strategy}"
    end
  end

  defp valid_backoff_strategy?(backoff_strategy), do: (backoff_strategy in @backoff_strategies)
end
