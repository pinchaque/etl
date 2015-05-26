require 'rails_helper'



class SuccessJob < Job
  attr_accessor :num_rows_success, :num_rows_error

  def run(batch_date)
    @num_rows_success = 34
    @num_rows_error = 1
  end
end

class ErrorJob < Job
  attr_accessor :num_rows_success, :num_rows_error

  def run(batch_date)
    @num_rows_success = 10
    @num_rows_error = 100
  end
end


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

  it "runs job - success" do
    job = SuccessJob.new
    job.id = 123
    batch = Date.new(2015, 3, 31)

    jr = JobRun.create_for_job(job, batch)
    jr.run()

    # check this object
    expect(jr.job_id).to eq(123)
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(34)
    expect(jr.num_rows_error).to eq(1)
  end

end
