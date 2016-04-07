
class JobRunSpecJob < ETL::Job::Base
  register_job
  def output_params
    { success: 34, error: 1, message: 'congrats!', sleep: nil, exception: nil }
  end
end


RSpec.describe "models/job_run" do
  
  before(:each) do
    ETL::Model::JobRun.dataset.delete
  end
  
  let(:batch) { ETL::Batch.new({ day: "2015-03-31" }) }
  let(:job) { JobRunSpecJob.new(batch) }
  let(:jr) { ETL::Model::JobRun }
  let(:tfmt) { "%F %T" }  
  let(:base_time) { Time.utc(2015, 3, 31, 8, 12, 34) }
  
  it "properly returns jobs" do
    expect(jr.find(job, batch).count).to eq(0)
    expect(jr.was_successful?(job, batch)).to be_falsy
    expect(jr.has_pending?(job, batch)).to be_falsy
    
    # first one errored out
    pretend_now_is(base_time + 0) do
      jm_error = jr.create_for_job(job, batch).error(ETL::Job::Result.error)
    end
    
    # there are no successful jobs
    expect(jr.find(job, batch).count).to eq(1)
    expect(jr.was_successful?(job, batch)).to be_falsy
    expect(jr.has_pending?(job, batch)).to be_falsy
    
    # then we had a success
    pretend_now_is(base_time + 10) do
      jm_success = jr.create_for_job(job, batch).success(ETL::Job::Result.success)
    end
    
    expect(jr.find(job, batch).count).to eq(2)
    expect(jr.was_successful?(job, batch)).to be_truthy
    expect(jr.has_pending?(job, batch)).to be_falsy
    
    # and then we queued a job
    pretend_now_is(base_time + 20) do
      jm_queued = jr.create_for_job(job, batch).queued
    end
    
    expect(jr.find(job, batch).count).to eq(3)
    expect(jr.was_successful?(job, batch)).to be_truthy
    expect(jr.has_pending?(job, batch)).to be_truthy
    
    # now there's a pending job
    expect(jr.has_pending?(job, batch)).to be_truthy
    jms = jr.find_pending(job, batch)
    expect(jms.count).to eq(1)
    expect(jms[0].status).to eq("queued")
    expect(jms[0].queued_at.utc.strftime(tfmt)).to eq((base_time + 20).strftime(tfmt))
    
    # latest finished should be that successful job
    jm = jr.last_ended(job, batch)
    expect(jm.status).to eq("success")
    expect(jm.ended_at.utc.strftime(tfmt)).to eq((base_time + 10).strftime(tfmt))
  end
end
