defmodule JobQueue.Util do
  def to_module(worker) when is_binary(worker) do
    worker
    |> String.split(".")
    |> Module.safe_concat()
  end

  def to_module(worker) when is_atom(worker), do: worker
end
