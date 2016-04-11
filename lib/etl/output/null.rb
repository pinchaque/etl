module ETL::Output

  # Dummy ETL class that is used for testing or other behavior simulation
  # Caller can set up number of seconds to sleep or the exception to throw
  # (to simulate error)
  class Null < Base
    attr_accessor :success, :sleep_time, :exception, :message
    
    # Initialize with the values we will use for the result
    def initialize
      super()
      @success = 0
      @sleep_time = nil
      @exception = nil
      @message = ''
    end

    def run_internal
      sleep(@sleep_time) unless @sleep_time.nil?
      raise ETL::OutputError, @exception unless @exception.nil?
      ETL::Job::Result.success(@success, @message)
    end
  end
end
