module ETL::Job

  # Base class for all ETL jobs
  class Base
    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = Job.register(self.class.to_s())
    end

    # Runs the job for the batch_date, keeping the status updated and handling
    # exceptions.
    def run(batch_date)
      jr = model().create_run(batch_date)

      begin
        jr.running()
        result = run_internal(batch_date)
        jr.success(result)
      rescue Exception => ex
        result = Result.new
        result.message = ex.message
        jr.error(result)
      end

      return jr
    end
  end
end
