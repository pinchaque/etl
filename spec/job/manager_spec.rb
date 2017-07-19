require 'etl'
require 'etl/job_factory/base'
require 'etl/job/base'
require 'etl/job/exec'
require 'etl/job/manager'

module ManagerTests

  # Test job should now be able to be registered
  # via the factory
  class TestJob < ETL::Job::Base
    attr_accessor :p1
    def initialize(batch, id, p1)
      super(batch)
      @p1 = p1
      @id = id
    end
    def id
      @id
    end
  end

  class TestJob2 < ETL::Job::Base
    def initialize(b)
      super(b)
    end
  end

  class TestJobFactory < ::ETL::JobFactory::Base
    def create(job_id, batch)
      klass = ::ETL::Job::Manager.instance.get_class(job_id)
      klass.new(batch, job_id, "1")
    end
  end
end
RSpec.describe "manager" do
  context "job factory" do
    it "register job with its factory and and create it" do
      manager = ::ETL::Job::Manager.instance
      tjf = ManagerTests::TestJobFactory.new
      test_job_klass = Object.const_get("ManagerTests").const_get("TestJob")
      manager.register("TestJob1", test_job_klass, tjf)
      manager.register("TestJob2", test_job_klass, tjf)
      f = manager.get_class_factory("TestJob1")
      expect(f).to eq(tjf)

      # ensure the job can be creates leveraging the job factory
      job = ETL::Job::Exec.create_job("TestJob1", test_job_klass, ETL::Batch.new)
      expect(job.p1).to eq("1")
    end

    it "register job with no job factory and create it" do
      manager = ::ETL::Job::Manager.instance
      tj = ManagerTests::TestJob2.new(::ETL::Batch.new)
      manager.register(tj.id, tj.class)
      f = manager.get_class_factory(tj.id)
      expect(f).to eq(nil)
      # ensure the job can be creates leveraging the job factory
      job = ETL::Job::Exec.create_job(tj.id, tj.class, ETL::Batch.new)
    end
  end
end

