require 'etl/job/exec'
require 'etl/job/migratable_base'

class Job < ETL::Job::MigratableBase 
  register_job

  def target_version
    1
  end

  def migration_dir
    "#{Dir.pwd}/db"
  end

  def output
    o = super
    o.success = 34
    o.message = 'congrats!'
    o.sleep_time = nil
    o.exception = nil
    o
  end
end

RSpec.describe "migratablejob" do
  
  # Make migration_dir and put files to be executed in migrate
  before(:all) do
    Dir.mkdir 'db'
    f1 = File.open("#{Dir.pwd}/db/job_0001.rb", "w")
    s = <<-END
  module Migration
    class Job0001
      def up
        puts "test output up at version 1"
      end

      def down 
        puts "test output down at version 1"
      end
    end
  end
END
    f1 << s
    f1.close()

    f2 = File.open("#{Dir.pwd}/db/job_0002.rb", "w")
    s = <<-END
  module Migration
    class Job0002
      def up
        puts "test output up at version 2"
      end

      def down 
        puts "test output down at version 2"
      end
    end
  end
END
    f2 << s
    f2.close()
  end

  after(:all) do
    system( "rm -rf #{Dir.pwd}/db")
  end

  before(:each) do
    ETL::Model::JobRun.dataset.delete
  end
  
  let(:batch) { ETL::Batch.new() }
  let(:job_id) { 'job' }
  let(:payload) { ETL::Queue::Payload.new(job_id, batch) }
  let(:job) { Job.new(ETL::Batch.new(payload.batch_hash)) }
  let(:job_exec) { ETL::Job::Exec.new(payload) }
  
  context "migration" do
    it { expect(job.id).to eq("job") }
    it { expect( job.migration_files.length ).to eq(2) }
    it "#migrate up to 2" do
      allow(job).to receive(:deploy_version).and_return(0)
      allow(job).to receive(:target_version).and_return(2)
      expect { job.migrate }.to output("test output up at version 1\ntest output up at version 2\n").to_stdout
    end 

    it "#migrate down to 1" do
      allow(job).to receive(:deploy_version).and_return(2)
      allow(job).to receive(:target_version).and_return(0)
      expect { job.migrate }.to output("test output down at version 2\ntest output down at version 1\n").to_stdout
    end 
  
    it "creates run models" do
      jr = ETL::Model::JobRun.create_for_job(job, batch)
      expect(jr.job_id).to eq('job')
      expect(jr.status).to eq("new")
      expect(jr.started_at).to be_nil
      expect(jr.batch).to eq(batch.to_json)
    end

    it "successful run" do
      ENV["JOB_SCHEMA_VERSION"] = "0"
      jr = job_exec.run

      # check this object
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be <= 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.rows_processed).to eq(job.output.success)
      expect(jr.message).to eq(job.output.message)
    end
  end
end