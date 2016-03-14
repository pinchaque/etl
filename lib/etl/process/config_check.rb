require 'etl/process/base'
require 'pp'

module ETL::Process

  class ConfigCheck < Base
    REDACT = "[REDACTED]"
    
    def run_process
      puts("===== Configuration =====")
      pp(ETL::HashUtil.sanitize(config.core, REDACT))
      puts("\n\n")
      
      puts("===== Databases =====")
      pp(ETL::HashUtil.sanitize(config.db, REDACT))
      puts("\n\n")
    end
  end
end
