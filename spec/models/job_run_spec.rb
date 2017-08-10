require 'pg'
require 'erb'
require 'singleton'
require 'etl/job/result'
require_relative '../job_repository_helper'

class JobRunSpecJob < ETL::Job::Base
  register_job

  def output
    o = super
    o.success = 34
    o.message = 'congrats!'
    o.sleep_time = nil
    o.exception = nil
    o
  end
end

RSpec.describe "models/job_run_repository" do
  before :all do
    JobRunRepositoryHelper.instance.setup
  end
  before(:each) do
    JobRunRepositoryHelper.instance.delete_all
  end

  after:all do
    #JobRunRepositoryHelper.instance.teardown
  end

  let(:batch) { ETL::Batch.new({ day: "2015-03-31" }) }
  let(:job) { JobRunSpecJob.new(batch) }
  let(:jrr) { ETL::Model::JobRunRepository.new }
  let(:tfmt) { "%F %T" }
  let(:base_time) { Time.utc(2015, 3, 31, 8, 12, 34) }

  it "properly returns jobs" do
    r = jrr.all
    expect(jrr.find(job, batch).length).to eq(0)
    expect(jrr.was_successful?(job, batch)).to be_falsy
    expect(jrr.has_pending?(job, batch)).to be_falsy

    # first one errored out
    pretend_now_is(base_time + 0) do
      jm_error = jrr.create_for_job(job, batch).error(ETL::Job::Result.error)
    end

    # there are no successful jobs
    expect(jrr.find(job, batch).count).to eq(1)
    expect(jrr.was_successful?(job, batch)).to be_falsy
    expect(jrr.has_pending?(job, batch)).to be_falsy

    # then we had a success
    pretend_now_is(base_time + 10) do
      jm_success = jrr.create_for_job(job, batch).success(ETL::Job::Result.success)
    end

    expect(jrr.find(job, batch).count).to eq(2)
    expect(jrr.was_successful?(job, batch)).to be_truthy
    expect(jrr.has_pending?(job, batch)).to be_falsy

    # and then we queued a job
    pretend_now_is(base_time + 20) do
      jm_queued = jrr.create_for_job(job, batch).queued
    end

    expect(jrr.find(job, batch).count).to eq(3)
    expect(jrr.was_successful?(job, batch)).to be_truthy
    r = jrr.has_pending?(job, batch)
    expect(r).to be_truthy

    # now there's a pending job
    expect(jrr.has_pending?(job, batch)).to be_truthy
    jms = jrr.find_pending(job, batch)
    expect(jms.count).to eq(1)
    expect(jms[0].status).to eq("queued")
    expect(jms[0].queued_at.utc.strftime(tfmt)).to eq((base_time + 20).strftime(tfmt))

    # latest finished should be that successful job
    jm = jrr.last_ended(job, batch)
    expect(jm.status).to eq("success")
    expect(jm.ended_at.utc.strftime(tfmt)).to eq((base_time + 10).strftime(tfmt))
  end
end
