module ETL::Job

  # Class that represents the result of an ETL job run
  class Result
    attr_accessor :num_rows_success, :num_rows_error, :message

    def initialize(success, error, msg)
      @num_rows_success = success
      @num_rows_error = error
      @message = msg
    end
  end
end
