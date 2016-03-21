module ETL
  module CachedLogger
    def log
      @log ||= ETL.create_logger(log_context)
    end
    
    def log=(l)
      @log = l
    end
    
    def log_context
      {}
    end
  end
end
