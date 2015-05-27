module ETL::Job

  # Dummy ETL class that is used for testing or other behavior simulation
  # Caller can set up number of seconds to sleep or the exception to throw
  # (to simulate error)
  class Dummy < Base
    attr_accessor :exception, :sleep

    # Initialize with the values we will use for the result
    def initialize(rows_success = 0, rows_error = 0, msg = '')
      @rows_success = rows_success
      @rows_error = rows_error
      @msg = msg
    end

    def run_internal(batch_date)
      r = Result.new(@rows_success, @rows_error, @msg)
      sleep @sleep unless @sleep.nil?
      raise @exception unless @exception.nil?
      return r
    end
  end
end
