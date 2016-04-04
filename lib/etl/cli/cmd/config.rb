require_relative '../command'
require 'pp'

module ETL::Cli::Cmd
  class Config < ETL::Cli::Command
    def execute
      puts("===== Configuration =====")
      pp(config_core_sanitized)
      puts("\n\n")
      
      puts("===== Databases =====")
      pp(config_db_sanitized)
      puts("\n\n")
    end
  end
end
