require 'rails_helper'

require 'etl/core'

RSpec.describe JobRun, :type => :model do

  it "creates runs for job" do
    job = Job.new
    job.id = 123

    batch = Date.new(2015, 3, 31)

    jr = JobRun.create_for_job(job, batch)

    expect(jr.job_id).to eq(123)
    expect(jr.status).to eq(:new)
    expect(jr.run_start_time).to be_nil
    expect(jr.batch_date).to eq(batch)
  end

  it "runs job - success" do
    Job.register("ETL::Job::Success")

    a = 34
    b = 1
    m = 'congrats!'

    job = ETL::Job::Success.new(a, b, m)
    batch = Date.new(2015, 3, 31)
    jr = job.run(batch)

    # check this object
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(a)
    expect(jr.num_rows_error).to eq(b)
    expect(jr.message).to eq(m)
  end

end
