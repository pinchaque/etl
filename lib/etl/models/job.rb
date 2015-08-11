require 'sequel'

module ETL::Model
  class Job < ::Sequel::Model
    # Register specified class as a job if it doesn't already exist
    def self.register(class_name)
      r = self.find(class_name: class_name)
      if r.nil?
          r = Job.new
          r.class_name = class_name
          r.save()
      end
      return r
    end

    # Creates JobRun object for this Job and specified batch
    def create_run(batch)
      jr = JobRun.create_for_job(self, batch)
      jr.save
      jr
    end
  end
  
  Job.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
