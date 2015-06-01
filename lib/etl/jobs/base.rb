module ETL::Job

  # Base class for all ETL jobs
  class Base
    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = Job.register(self.class.to_s())
    end

    def logger
      Rails.logger
    end

    # Runs the job for the batch_id, keeping the status updated and handling
    # exceptions.
    def run(batch_id)
      jr = model().create_run(batch_id)

      log_prefix = "[Job=#{model().class_name} / Batch=#{batch_id}] "

      begin
        logger.info(log_prefix + "Running...")
        jr.running()
        result = run_internal(batch_id)
        logger.info(log_prefix + "Success! #{result.num_rows_success} rows; "\
          + "#{result.num_rows_error} errors; #{result.message}")
        jr.success(result)
      rescue Exception => ex
        logger.error(log_prefix + "Error: #{ex}")
        ex.backtrace.each do |x|
          logger.error(log_prefix + "    #{x}")
        end
        result = Result.new
        result.message = ex.message
        jr.error(result)
      end

      return jr
    end
  end
end
