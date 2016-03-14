
class SpecJob < ETL::Job::Base
  def input_class
    ETL::Input::Null
  end
  
  def output_class
    ETL::Output::Null
  end
  
  def output_params
    { success: 34, error: 1, message: 'congrats!', sleep: nil, exception: nil }
  end
end

class SpecJobSleep < ETL::Job::Base
  def output_params
    { success: 35, error: 2, message: 'congrats!', sleep: 2, exception: nil }
  end
end

class SpecJobError < ETL::Job::Base
  def output_params
    { success: 1, error: 100, message: 'error!', sleep: nil, exception: 'abort!' }
  end
end



RSpec.describe "job" do
  
  before(:each) do
    ETL::Model::JobRun.dataset.delete
  end
  
  let(:batch) { ETL::Batch.new({ day: "2015-03-31" }) }
  let(:job_class) { 'SpecJob' }
  let(:payload) {
    # run this job by creating payload and then using ETL::Job
    payload = ETL::Queue::Payload.new
    payload.job_id = job_class
    payload.batch = batch
    payload
  }
  let(:job) { SpecJob.new(payload.batch) }
  let(:job_exec) { ETL::JobExec.new(payload) }
  
  it "creates run models" do
    jr = ETL::Model::JobRun.create_for_job(job, batch)
    expect(jr.job_class).to eq('SpecJob')
    expect(jr.status).to eq("new")
    expect(jr.run_start_time).to be_nil
    expect(jr.batch).to eq(batch.to_json)
  end

  it "successful run" do
    jr = job_exec.run

    # check this object
    expect(jr.job_class).to eq(job.class.name)
    expect(jr.status).to eq("success")
    expect(jr.queued_at).to be_nil
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be <= 1
    expect(jr.batch).to eq(batch.to_json)
    expect(jr.num_rows_success).to eq(job.output_params[:success])
    expect(jr.num_rows_error).to eq(job.output_params[:error])
    expect(jr.message).to eq(job.output_params[:message])
    
    # now check what's in the db
    runs = ETL::Model::JobRun.where(job_class: job.class.name).all
    expect(runs.count).to eq(1)
    jr = runs[0]
    expect(jr.job_class).to eq(job.class.name)
    expect(jr.status).to eq("success")
    expect(jr.queued_at).to be_nil
    expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be <= 1
    expect(jr.batch).to eq(batch.to_json)
    expect(jr.num_rows_success).to eq(job.output_params[:success])
    expect(jr.num_rows_error).to eq(job.output_params[:error])
    expect(jr.message).to eq(job.output_params[:message])
  end

  describe "successful run with sleep" do
    let(:job_class) { 'SpecJobSleep' }
    let(:job) { SpecJobSleep.new(payload.batch) }

    it 'sets correct result state' do
      jr = job_exec.run
      expect(jr.job_class).to eq(job.class.name)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be > 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to eq(job.output_params[:success])
      expect(jr.num_rows_error).to eq(job.output_params[:error])
      expect(jr.message).to eq(job.output_params[:message])
    
      runs = ETL::Model::JobRun.where(job_class: job.class.name).all
      expect(runs.count).to eq(1)
      jr = runs[0]
      expect(jr.job_class).to eq(job.class.name)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_end_time.to_i() - jr.run_start_time.to_i()).to be > 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to eq(35)
      expect(jr.num_rows_error).to eq(2)
      expect(jr.message).to eq('congrats!')
    end
  end

  describe "run with exception" do
    let(:job_class) { 'SpecJobError' }
    let(:job) { SpecJobError.new(payload.batch) }

    it 'sets correct result state' do
      jr = job_exec.run
      
      expect(jr.job_class).to eq(job.class.name)
      expect(jr.status).to eq("error")
      expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to be_nil
      expect(jr.num_rows_error).to be_nil
      expect(jr.message.start_with?('abort!')).to be_truthy
      expect(jr.message.match(/null.rb:\d+:in `run_internal/)).to be_truthy
      
      runs = ETL::Model::JobRun.where(Sequel.expr(job_class: job.class.name)).all
      expect(runs.count).to eq(1)
      jr = runs[0]
      expect(jr.job_class).to eq(job.class.name)
      expect(jr.status).to eq("error")
      expect(jr.run_end_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.run_start_time.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to be_nil
      expect(jr.num_rows_error).to be_nil
      expect(jr.message.start_with?('abort!')).to be_truthy
      expect(jr.message.match(/null.rb:\d+:in `run_internal/)).to be_truthy
    end
  end
end
