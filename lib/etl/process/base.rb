require 'optparse'
require 'etl/mixins/cached_logger'

module ETL::Process
  
  # Base class for the various processes we run in the ETL system
  class Base
    include ETL::CachedLogger
    
    def initialize
      @options = {}
    end
    
    def config
      ETL.config
    end
    
    def queue
      ETL.queue
    end
    
    def run_process
      # do nothing
    end
    
    # Starts the infinite loop that processes jobs from the queue
    def start
      parse_argv
      name = self.class.name.to_s
      log.info("Starting #{name}")
      begin
        run_process
      rescue Exception => ex
        log.exception(ex)
      ensure
        log.info("Exiting #{name}")
      end
    end
    
    def option_parser
      OptionParser.new do |opts|
        opts.on("-c", "--config DIR", "Configuration directory") do |dir|
          @options[:config] = dir
        end
      end
    end
    
    def required_args
      [:config]
    end
    
    def parse_argv
      option_parser.parse(ARGV)
      
      required_args.each do |opt|
        unless @options[opt]
          puts(option_parser)
          abort("\nERROR: Option '#{opt}' is required") 
        end
      end
      
      config.config_dir = @options[:config]
      log.debug("Using main config file: #{config.core_file}")
      log.debug("Using database config file: #{config.db_file}")
    end
  end
end
