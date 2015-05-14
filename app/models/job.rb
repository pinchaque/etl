class Job < ActiveRecord::Base
  has_many :job_runs

  public:

  def run(batch_date)
    jr = JobRun.new
    jr.jobs_id = this.id
    jr.job_statuses_id =  JobRunStatus.id_from_label(:new)
    jr.run_start_time = DateTime.now
    jr.batch_date = batch_date
    jr.save
  end
end
