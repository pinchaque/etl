require 'sequel'

module ETL::Model
  class JobRun < ::Sequel::Model

    # Creates JobRun object from Job and batch date
    def self.create_for_job(job, batch)
      JobRun.new do |jr|
        jr.job_id = job.id
        jr.status = :new
        jr.batch = batch.to_json
      end
    end
    
    # Finds all runs for specified job and batch
    def self.find(job, batch)
      JobRun.where(job_id: job.id, batch: batch.to_json).all
    end
    
    # Finds all "pending" runs for specified job and batch
    # Pending means the job is either queued or currently running
    def self.find_pending(job, batch)
      JobRun.where(
        job_id: job.id, 
        batch: batch.to_json,
        status: %w(queued running)
        ).all
    end
    
    # Returns true if this job+batch has pending jobs
    def self.has_pending?(job, batch)
      !self.find_pending(job, batch).empty?
    end
    
    # Returns whether there have been any successful runs of this job+batch
    def self.was_successful?(job, batch)
      !JobRun.where(
        job_id: job.id, 
        batch: batch.to_json,
        status: "success"
        ).first.nil?
    end
    
    # Returns the last ended JobRun, or nil if none has ended
    # Note that a ended job can be either success or error
    def self.last_ended(job, batch)
      JobRun.where(
        job_id: job.id, 
        batch: batch.to_json,
        status: %w(success error)
        ).order(:ended_at).last
    end

    # Sets the current status as queued and sets queued_at
    def queued
      self.status = :queued
      self.queued_at = Time.now
      save()
    end

    # Sets the current status as running and initializes started_at
    def running
      self.status = :running
      self.started_at = Time.now
      save()
    end

    # Sets the final status as success along with rows affected
    def success(result)
      final_state(:success, result)
    end

    # Sets the final status as error along with rows affected
    def error(result)
      final_state(:error, result)
    end
    
    # Mark this as an error and save the exception message
    def exception(ex)
      error(ETL::Job::Result.new(nil, nil, ex.message + "\n" + ex.backtrace.join("\n")))
    end

    def success?
      self.status == :success
    end

    private
    def final_state(state, result)
      self.status = state
      self.ended_at = Time.now
      self.num_rows_success = result.num_rows_success
      self.num_rows_error = result.num_rows_error
      self.message = result.message
      save()
    end
  end
  
  JobRun.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
