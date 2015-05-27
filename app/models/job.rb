class Job < ActiveRecord::Base
  has_many :job_runs

  # Register specified class as a job if it doesn't already exist
  def self.register(class_name)
    r = self.find_by(class_name: class_name)
    if r.nil?
        r = Job.new
        r.class_name = class_name
        r.save()
    end
    return r
  end

  # Creates JobRun object for this Job and specified batch_date
  def create_run(batch_date)
    jr = JobRun.create_for_job(self, batch_date)
    jr.save
    jr
  end
end
