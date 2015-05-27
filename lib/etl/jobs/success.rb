module ETL::Job
  class Success < Base

    def initialize(rows_success = 0, rows_error = 0, msg = '')
      @rows_success = rows_success
      @rows_error = rows_error
      @msg = msg
    end

    def run_internal(batch_date)
      Result.new(@rows_success, @rows_error, @msg)
    end
  end
end
