module ETL

  # Class that runs jobs given a payload
  class Job
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
      batch, jm = extract_payload
      
      # instantiate the input class
      input = Object::const_get(jm.input_class).new(jm.input_params_hash)
      
      # prepare the job to run
      output = Object::const_get(jm.output_class).new(jm.output_params_hash)
      output.reader = input
      output.feed_name = jm.feed_name
      output.batch = batch
      
      # get a run for this job
      jr = ETL::Model::JobRun.create_for_job(jm, batch)
      
      # change status to running
      jr.running()
        
      begin
        result = output.run()
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
      ETL.logger
    end

    def extract_payload
      # Extract info from payload
      job_model = @payload.job_model
      raise ETL::JobError, "Invalid job_id in payload: '#{@payload.job_id}'" unless job_model
      [@payload.batch, job_model]
    end
  end
end
