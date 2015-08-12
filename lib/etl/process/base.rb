module ETL::Process

  # Base class for the various processes we run in the ETL system
  class Base
    def initialize
    end
    
    def log
      ETL.logger
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
      name = self.class.to_s
      log.info("Starting #{name}")
      begin
        run_process
      rescue Exception => ex
        log.exception(ex)
      ensure
        log.info("Exiting #{name}")
      end
    end
  end
end
