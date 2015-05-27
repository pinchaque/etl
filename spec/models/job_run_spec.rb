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
    a = 34
    b = 1
    m = 'congrats!'

    job = ETL::Job::Dummy.new(a, b, m)
    batch = Date.new(2015, 3, 31)
    jr = job.run(batch)

    # check this object
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be <= 1
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(a)
    expect(jr.num_rows_error).to eq(b)
    expect(jr.message).to eq(m)

    # now check what's in the db
    runs = JobRun.where(job_id: job.model.id)
    expect(runs.count).to eq(1)
    jr = runs[0]
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be <= 1
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(a)
    expect(jr.num_rows_error).to eq(b)
    expect(jr.message).to eq(m)
  end

  it "runs job - sleep" do
    a = 34
    b = 1
    m = 'congrats!'

    job = ETL::Job::Dummy.new(a, b, m)
    job.sleep_time = 3 # seconds
    batch = Date.new(2015, 3, 31)
    jr = job.run(batch)

    # check this object
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be > 1
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(a)
    expect(jr.num_rows_error).to eq(b)
    expect(jr.message).to eq(m)
    
    # now check what's in the db
    runs = JobRun.where(job_id: job.model.id)
    expect(runs.count).to eq(1)
    jr = runs[0]
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:success)
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be > 1
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to eq(a)
    expect(jr.num_rows_error).to eq(b)
    expect(jr.message).to eq(m)
   end

  it "runs job - error" do
    a = 1
    b = 100
    m = 'error!'

    job = ETL::Job::Dummy.new(a, b, m)
    job.exception = 'abort!'
    batch = Date.new(2015, 3, 31)
    jr = job.run(batch)

    # check this object
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:error)
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to be_nil
    expect(jr.num_rows_error).to be_nil
    expect(jr.message).to eq(job.exception)
    
    # now check what's in the db
    runs = JobRun.where(job_id: job.model.id)
    expect(runs.count).to eq(1)
    jr = runs[0]
    expect(jr.job_id).to eq(job.model.id)
    expect(jr.status).to eq(:error)
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.batch_date).to eq(batch)
    expect(jr.num_rows_success).to be_nil
    expect(jr.num_rows_error).to be_nil
    expect(jr.message).to eq(job.exception)
   end

end
