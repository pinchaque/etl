require 'singleton'

module ETL::Job
  class Manager
    include Singleton

    # Map of id => Class
    attr_accessor :job_classes, :job_class_factories

    def initialize
      @job_classes = {}
      @job_class_factories = {}
    end

    # Registers a job class with the manager. This is typically called by
    # subclasses to register themselves with a convenient id to represent
    # that subclass. Only registered jobs can be executed.
    def register(id, klass, klass_factory=nil)
      id_str = id.to_s
      ETL.logger.debug("Registering job class with manager: #{id_str} => #{klass}")
      if @job_classes.has_key?(id)
        ETL.logger.warn("Overwriting previous registration of: #{id_str} => #{@job_classes[id_str]}")
      end

      if !klass_factory.nil?
        ETL.logger.debug("Registering job class factory with manager: #{id_str}")
        if @job_classes.has_key?(id_str)
          ETL.logger.warn("Overwriting previous registration of job factory: #{id_str}")
        end
        @job_class_factories[id_str] = klass_factory
      end

      @job_classes[id_str] = klass
    end

    # Returns the job class registered for the specified ID, or nil if none
    def get_class(id)
      @job_classes[id.to_s]
    end

    # Returns the job class factory registered for the specified ID, or nil if none
    def get_class_factory(job_id)
      @job_class_factories[job_id.to_s]
    end

    # Iterates through the loaded classes specified during class initialization.
    def each_class(&block)
      @job_classes.each do |id, klass|
        yield klass
      end
    end
  end
end
