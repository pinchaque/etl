module ETL::Output

  # Class that represents the result of an ETL job run
  class Result
    attr_accessor :num_rows_success, :num_rows_error, :message

    def initialize(rows_success = nil, rows_error = nil, msg = '')
      @num_rows_success = rows_success
      @num_rows_error = rows_error
      @message = msg
    end
  end
end
