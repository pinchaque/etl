require 'json'
require 'sequel'

module ETL::Model
  class JobRun < ::Sequel::Model
    attr_accessor :job

    # Creates JobRun object from Job and batch date
    def self.create_for_job(job, batch)
      JobRun.new do |jr|
        jr.job = job
        jr.job_id = job.id
        jr.status = :new
        jr.batch = batch.to_json
      end
    end

    # Sets status for this job given label
    def status=(label)
      self.job_run_status_id = JobRunStatus.id_from_label(label)
    end

    # Gets status for this job as the label
    def status()
      JobRunStatus.label_from_id(self.job_run_status_id)
    end

    # Sets the current status as running and initializes run_start_time
    def running()
      self.status = :running
      self.run_start_time = DateTime.now
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
    
    def success?
      self.status == :success
    end

    private
    def final_state(state, result)
      self.status = state
      self.run_end_time = DateTime.now
      self.num_rows_success = result.num_rows_success
      self.num_rows_error = result.num_rows_error
      self.message = result.message
      save()
    end
  end
  
  JobRun.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
