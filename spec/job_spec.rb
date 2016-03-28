require 'etl/job/exec'

class SpecDefaultJob < ETL::Job::Base
  public :log_context # for testing
end

class SpecJob < ETL::Job::Base
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
  let(:job_id) { 'SpecJob' }
  let(:payload) { ETL::Queue::Payload.new(job_id, batch) }
  let(:job) { SpecJob.new(payload.batch) }
  let(:job_exec) { ETL::Job::Exec.new(payload) }
  
  it "has sane default settings" do
    expect(SpecDefaultJob.schedule_class).to eq(ETL::Schedule::Never)
    expect(SpecDefaultJob.input_class).to eq(ETL::Input::Null)
    expect(SpecDefaultJob.output_class).to eq(ETL::Output::Null)
    expect(SpecDefaultJob.batch_factory_class).to eq(ETL::BatchFactory::Base)
    
    j = SpecDefaultJob.new(ETL::Batch.new)
    expect(j.id).to eq("SpecDefaultJob")
    expect(j.to_s).to eq("SpecDefaultJob<>")
    expect(j.schedule.ready?).to be_falsy
    expect(j.log_context).to eq({ job: "SpecDefaultJob", batch: "" })
  end
  
  it "creates run models" do
    jr = ETL::Model::JobRun.create_for_job(job, batch)
    expect(jr.job_id).to eq('SpecJob')
    expect(jr.status).to eq("new")
    expect(jr.started_at).to be_nil
    expect(jr.batch).to eq(batch.to_json)
  end

  it "successful run" do
    jr = job_exec.run

    # check this object
    expect(jr.job_id).to eq(job.id)
    expect(jr.status).to eq("success")
    expect(jr.queued_at).to be_nil
    expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be <= 1
    expect(jr.batch).to eq(batch.to_json)
    expect(jr.num_rows_success).to eq(job.output_params[:success])
    expect(jr.num_rows_error).to eq(job.output_params[:error])
    expect(jr.message).to eq(job.output_params[:message])
    
    # now check what's in the db
    runs = ETL::Model::JobRun.where(job_id: job.id).all
    expect(runs.count).to eq(1)
    jr = runs[0]
    expect(jr.job_id).to eq(job.id)
    expect(jr.status).to eq("success")
    expect(jr.queued_at).to be_nil
    expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
    expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be <= 1
    expect(jr.batch).to eq(batch.to_json)
    expect(jr.num_rows_success).to eq(job.output_params[:success])
    expect(jr.num_rows_error).to eq(job.output_params[:error])
    expect(jr.message).to eq(job.output_params[:message])
  end

  describe "successful run with sleep" do
    let(:job_id) { 'SpecJobSleep' }
    let(:job) { SpecJobSleep.new(payload.batch) }

    it 'sets correct result state' do
      jr = job_exec.run
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be > 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to eq(job.output_params[:success])
      expect(jr.num_rows_error).to eq(job.output_params[:error])
      expect(jr.message).to eq(job.output_params[:message])
    
      runs = ETL::Model::JobRun.where(job_id: job.id).all
      expect(runs.count).to eq(1)
      jr = runs[0]
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be > 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to eq(35)
      expect(jr.num_rows_error).to eq(2)
      expect(jr.message).to eq('congrats!')
    end
  end

  describe "run with exception" do
    let(:job_id) { 'SpecJobError' }
    let(:job) { SpecJobError.new(payload.batch) }

    it 'sets correct result state' do
      jr = job_exec.run
      
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("error")
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to be_nil
      expect(jr.num_rows_error).to be_nil
      expect(jr.message.start_with?('abort!')).to be_truthy
      expect(jr.message.match(/null.rb:\d+:in `run_internal/)).to be_truthy
      
      runs = ETL::Model::JobRun.where(Sequel.expr(job_id: job.id)).all
      expect(runs.count).to eq(1)
      jr = runs[0]
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("error")
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.num_rows_success).to be_nil
      expect(jr.num_rows_error).to be_nil
      expect(jr.message.start_with?('abort!')).to be_truthy
      expect(jr.message.match(/null.rb:\d+:in `run_internal/)).to be_truthy
    end
  end
end
