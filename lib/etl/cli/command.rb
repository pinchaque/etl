require 'clamp'
require 'etl/mixins/cached_logger'

module ETL::Cli

  class Command < Clamp::Command

    option ["-l", "--log-level"], "LOG_LEVEL", "log level", default: 'info' do |level|
      ETL.config.core[:log][:level] = level
    end
    
    option ["-c", "--config-dir"], "CONFIG_DIR", "configuration directory", default: "#{ETL.root}/etc" do |dir|
      config.config_dir = dir
      log.debug("Using main config file: #{config.core_file}")
      log.debug("Using database config file: #{config.db_file}")
      dir
    end
    
    option ["-v", "--version"], :flag, "show version" do
      puts("ETL system version #{ETL::VERSION}")
      exit(0)
    end

    protected
    
    include ETL::CachedLogger
    
    def config
      ETL.config
    end
    
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
