class JobRun < ActiveRecord::Base
  belongs_to :job
  attr_accessor :job

  # Creates JobRun object from Job and batch date
  def self.create_for_job(job, batch_date)
    JobRun.new do |jr|
      jr.job = job
      jr.job_id = job.id
      jr.status = :new
      jr.run_start_time = DateTime.now
      jr.batch_date = batch_date
    end
  end

  # Sets status for this job given label
  def status=(label)
    id = JobRunStatus.id_from_label(label)
    self.job_run_status_id = id
  end

  # Gets status for this job as the label
  def status()
    JobRunStatus.label_from_id(self.job_run_status_id)
  end


  # Runs the job for the batch_date, keeping the status updated and handling
  # exceptions.
  def run()
    begin
      self.status = :running
      self.save()

      @job.run(self.batch_date)

      self.status = :success
      self.run_end_time = DateTime.now
      self.num_rows_success = @job.num_rows_success
      self.num_rows_error = @job.num_rows_error
      self.save()

    rescue Exception => ex
      puts(ex)
      self.status = :error
      self.run_end_time = DateTime.now
      self.num_rows_success = @job.num_rows_success
      self.num_rows_error = @job.num_rows_error
      self.save()
    end
  end
end
