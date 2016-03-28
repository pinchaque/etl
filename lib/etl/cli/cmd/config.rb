require_relative '../command'
require 'pp'

module ETL::Cli::Cmd
  class Config < ETL::Cli::Command
    REDACT = "[REDACTED]"
    
    def execute
      puts("===== Configuration =====")
      pp(ETL::HashUtil.sanitize(config.core, REDACT))
      puts("\n\n")
      
      puts("===== Databases =====")
      pp(ETL::HashUtil.sanitize(config.db, REDACT))
      puts("\n\n")
    end
  end
end
