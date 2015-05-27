module ETL::Job
  class Error < Base
    def run(batch_date)
      @num_rows_success = 10
      @num_rows_error = 100
      raise "Forced error"
    end
  end
end
