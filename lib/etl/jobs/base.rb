module ETL::Job

  # Base class for all ETL jobs
  class Base
    @job_model = nil

    def job_model()
      # return the job if we already have it cached in this instance
      return @job unless @job.nil?

      # get the job out of the DB
      @job = Job.register(self.class.to_s())
    end

    # Runs the job for the batch_date, keeping the status updated and handling
    # exceptions.
    def run(batch_date)
      @job_model = job_model()

      jr = @job_model.create_run(batch_date)

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
