require_relative './command'

module ETL::Cli
  class Main < Command

    require_relative './cmd/config'
    subcommand 'config', "Checks on ETL system configuration ", Cmd::Config

    require_relative './cmd/queue'
    subcommand 'queue', "Commands for managing job queue", Cmd::Queue

    require_relative './cmd/scheduler'
    subcommand 'scheduler', "Process for enqueuing ETL jobs", Cmd::Scheduler

    require_relative './cmd/schema'
    subcommand 'schema', "Manages schema for ETL system jobs", Cmd::Schema

    require_relative './cmd/worker'
    subcommand 'worker', "Process for executing queued ETL jobs", Cmd::Worker
  end
end
