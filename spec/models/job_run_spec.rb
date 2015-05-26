require 'rails_helper'

RSpec.describe JobRun, :type => :model do

  it "creates runs for job" do
    job = Job.new
    job.id = 123

    batch = Date.new(2015, 3, 31)

    jr = JobRun.create_for_job(job, batch)

    expect(jr.job_id).to eq(123)
    expect(jr.status).to eq(:new)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.batch_date).to eq(batch)
  end

end
