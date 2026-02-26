import Config

log_level =
  if System.get_env("FELINE_DEBUG") do
    :debug
  else
    :info
  end

config :logger, level: log_level
