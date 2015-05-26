class JobRun < ActiveRecord::Base
  belongs_to :job

  # Creates JobRun object from Job and batch date
  def self.create_for_job(job, batch_date)
    JobRun.new do |jr|
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
end
