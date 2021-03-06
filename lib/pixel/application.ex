defmodule Pixel.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT") || "4040")
    # List all child processes to be supervised
    children = [
      {Task.Supervisor, name: Pixel.Server.Supervisor},
      Supervisor.child_spec({Task, fn -> Pixel.Server.accept(port) end}, restart: :permanent)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Pixel.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
