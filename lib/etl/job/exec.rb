module ETL::Job

  # Class that runs jobs given a payload
  class Exec
    # Initialize with payload we received from the queue
    def initialize(payload, params = {})
      @payload = payload
      @params = {
        retry_max: 5,
        retry_wait: 4,
        retry_mult: 2.0,
      }.merge(params)
    end

    # Run the job for this object's payload and handles any immediate retries.
    # Saves job result in the DB and returns the ETL::Model::JobRun object
    # produced. If the job has an exception this will (a) store that in the
    # job messages; (b) log it; (c) swallow it.
    def run
      retries = 0
      retry_wait = @params[:retry_wait]
      
      # get batch and job model out of the payload
      batch, job = extract_payload
        
      # get a run for this job
      jr = ETL::Model::JobRun.create_for_job(job, batch)
      
      # change status to running
      jr.running()
        
      begin
        result = job.run()
        jr.success(result)
      rescue Sequel::DatabaseError => ex
        # By default we want to retry database errors...
        do_retry = true
        
        # But there are some that we know are fatal
        do_retry = false if ex.message.include?("Mysql2::Error: Unknown database")
        
        # Help debug timeouts by logging the full exception
        if ex.message.include?("Mysql2::Error: Lock wait timeout exceeded")
          log.exception(ex, Logger::WARN)
        end
        
        # Retry this job with exponential backoff
        retries += 1
        do_retry &&= (retries <= @params[:retry_max])
        if do_retry
          log.warn("Database error '#{ex.message}' on attempt " +
            "#{retries}/#{@params[:retry_max]}; waiting for #{retry_wait} seconds " +
            "before retrying")
          sleep(retry_wait)
          retry_wait *= @params[:retry_mult]
          retry
        end
        
        # we aren't retrying anymore - log this error
        jr.exception(ex)
      rescue StandardError => ex
        # for all other exceptions: save the message
        jr.exception(ex)
      end
      
      return jr
    end
    
    private

    def log
      @log ||= ETL.create_logger({
          job: @payload.job_id,
          batch: @payload.batch.to_s,
        })
    end

    def extract_payload
      # Extract info from payload
      klass = @payload.job_id
      job_obj = Object::const_get(klass).new(@payload.batch)
      raise ETL::JobError, "Invalid job_id in payload: '#{klass}'" unless job_obj
      [@payload.batch, job_obj]
    end
  end
end
