require 'clamp'
require 'etl/mixins/cached_logger'

module ETL::Cli

  class Command < Clamp::Command

    option ["-l", "--log-level"], "LOG_LEVEL", "log level", default: 'info' do |level|
      ETL.config.core[:log][:level] = level
    end
    
    option ["-c", "--config-dir"], "CONFIG_DIR", "configuration directory", default: "#{ETL.root}/etc" do |dir|
      config.config_dir = File.expand_path('.', dir)
      log_config
      config.config_dir
    end
  
    option ["-v", "--version"], :flag, "show version" do
      puts("ETL system version #{ETL::VERSION}")
      exit(0)
    end

    protected
    
    REDACT = "[REDACTED]"
    
    include ETL::CachedLogger
    
    def config
      ETL.config
    end
      
    def config_core_sanitized
      ETL::HashUtil.sanitize(config.core, REDACT)
    end
    
    def config_db_sanitized
      ETL::HashUtil.sanitize(config.db, REDACT)
    end
      
    def log_config
      log.debug("Using config dir: #{config.config_dir}")
      log.debug("Using main config file: #{config.core_file}")
      log.debug(config_core_sanitized)
      log.debug("Using database config file: #{config.db_file}")
      log.debug(config_db_sanitized)
    end
    
    # Wraps given block with logging to indicate when it is starting and 
    # ending. Also logs any exceptions that were raised and re-raises them.
    def with_log(&block)
      name = self.class.name.to_s
      log.info("Starting #{name}")
      begin
        yield
      rescue StandardError => ex
        log.exception(ex)
        raise
      ensure
        log.info("Exiting #{name}")
      end
    end
  end
end
