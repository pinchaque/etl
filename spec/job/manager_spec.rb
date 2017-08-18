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
      @job_id = id
    end
    def id
      @job_id
    end
  end

  class TestJob2 < ETL::Job::Base
  end

  class TestJobFactory < ::ETL::JobFactory::Base
    def create(job_id, batch)
      klass = ::ETL::Job::Manager.instance.get_class(job_id)
      klass.new(batch, job_id, "1")
    end
  end
end

module ManagerDependencyTests
  class A1 < ETL::Job::Base
  end

  class A2 < ETL::Job::Base
  end

  class A3 < ETL::Job::Base
  end

  class A4 < ETL::Job::Base
  end

  class C1 < ETL::Job::Base
  end

  class D1 < ETL::Job::Base
  end

  class C2 < ETL::Job::Base
  end

  class CD2 < ETL::Job::Base
  end

  class CD3 < ETL::Job::Base
  end

  class CD4 < ETL::Job::Base
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

  context "dependency jobs" do
    it "register job with its factory and and create it" do
      manager = ::ETL::Job::Manager.instance

      # Register root nodes
      a1 = ManagerDependencyTests::A1.new(::ETL::Batch.new)
      manager.register(a1.id, a1.class)
      c1 = ManagerDependencyTests::C1.new(::ETL::Batch.new)
      manager.register(c1.id, c1.class)
      d1 = ManagerDependencyTests::D1.new(::ETL::Batch.new)
      manager.register(d1.id, d1.class)

      # Register child nodes
      a2 = ManagerDependencyTests::A2.new(::ETL::Batch.new)
      manager.register_job_with_parent(a2.id, "a1", a2.class)
      a3 = ManagerDependencyTests::A3.new(::ETL::Batch.new)
      manager.register_job_with_parent(a3.id, "a1", a3.class)
      a4 = ManagerDependencyTests::A4.new(::ETL::Batch.new)
      manager.register_job_with_parent(a4.id, "a3", a4.class)

      cd2 = ManagerDependencyTests::CD2.new(::ETL::Batch.new)
      manager.register_job_with_parent(cd2.id, "c1", cd2.class)
      manager.register_job_with_parent(cd2.id, "d1", cd2.class)
      cd3 = ManagerDependencyTests::CD3.new(::ETL::Batch.new)
      manager.register_job_with_parent(cd3.id, "cd2", cd3.class)
      cd4 = ManagerDependencyTests::CD4.new(::ETL::Batch.new)
      manager.register_job_with_parent(cd4.id, "cd3", cd4.class)

      nodes = manager.sorted_dependent_jobs
      a1_index = nodes.index("a1") 
      a2_index = nodes.index("a2") 
      a3_index = nodes.index("a3") 
      a4_index = nodes.index("a4") 
      expect(a1_index).to be < a2_index 
      expect(a1_index).to be < a3_index 
      expect(a3_index).to be < a4_index 

      c1_index = nodes.index("c1") 
      d1_index = nodes.index("d1") 
      cd2_index = nodes.index("cd2") 
      cd3_index = nodes.index("cd3") 
      cd4_index = nodes.index("cd4") 
      expect(c1_index).to be < cd2_index 
      expect(d1_index).to be < cd2_index 
      expect(cd2_index).to be < cd3_index 
      expect(cd3_index).to be < cd4_index 
    end
  end
end

