class Job < ActiveRecord::Base
  has_many :job_runs

  public

  def run(batch_date)
    jr = JobRun.create_for_job(this, batch_date)
    jr.save
  end
end
