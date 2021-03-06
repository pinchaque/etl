require 'etl/models/job_run'

module ETL::Schedule
  
  # Base class for job schedules. This object is given a job ID + batch and
  # is responsible for determining if we should be queueing that job for
  # running.
  class Base

    def initialize(job, batch)
      @job = job
      @batch = batch
    end
    
    # Returns true if we should queue this job for executing given the rules
    # of this schedule object. Typically this would involve looking at the
    # recent runs of the job and/or the current time
    def ready?
      raise("Called #{self.class.name}.ready?")
    end
    
    # Returns true if there are any "pending" jobs - that is, jobs that are
    # queued or currently running
    def has_pending?
      self.class.job_run_class.has_pending?(@job, @batch)
    end
    
    # Returns true if there have been any successful runs of this job+batch
    def was_successful?
      self.class.job_run_class.was_successful?(@job, @batch)
    end
    
    # Allow changing the model class we use to get job run information
    # to make testing easier and for extensibility
    @@job_run_class = ETL::Model::JobRun
    def self.job_run_class; @@job_run_class; end
    def self.job_run_class=(c); @@job_run_class = c; end
  end
  
  # Runs jobs continuously - as soon as one is finished the next is ready to
  # run
  class Continuous < Base
    # we're ready to run if we don't have any pending jobs
    def ready?
      !has_pending?
    end
  end
  
  # Always runs the job, regardless of whether there are pending jobs
  class Always < Base
    def ready?
      true
    end
  end
   
  # Never runs the job
  class Never < Base
    def ready?
      false
    end
  end
  
  # Runs jobs once successfully per batch; won't start a new run if there is
  # a pending one
  class Once < Base
    def ready?
      !has_pending? && !was_successful?
    end
  end
end
