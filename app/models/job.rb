class Job < ActiveRecord::Base
  has_many :job_runs

  public

  # Creates JobRun object for this Job and specified batch_date
  def create_run(batch_date)
    JobRun.create_for_job(this, batch_date)
  end

  # Runs this job for the specified batch date
  def run(batch_date)
    raise "Job.run() not implemented"
  end
end
