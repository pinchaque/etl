require 'singleton'

module ETL::Job
  class Manager
    include Singleton
    
    # Map of id => Class
    attr_accessor :job_classes
    
    def initialize
      @job_classes = {}
    end
    
    # Registers a job class with the manager. This is typically called by
    # subclasses to register themselves with a convenient id to represent
    # that subclass. Only registered jobs can be executed.
    def register(id, klass)
      ETL.logger.debug("Registering job class with manager: #{id} => #{klass}")
      if @job_classes.has_key?(id)
        ETL.logger.warn("Overwriting previous registration of: #{id} => #{@job_classes[id]}")
      end
      @job_classes[id] = klass
    end
    
    # Returns the job class registered for the specified ID, or nil if none
    def get_class(id)
      @job_classes[id]
    end
    
    # Iterates through the loaded classes specified during class initailiation.
    def each_class(&block)
      @job_classes.each do |id, klass|
        yield klass
      end
    end
    
    # Function to load all job classes in the specified directory
    def self.load_job_classes(class_dir)
      unless class_dir.start_with?("/")
        class_dir = ETL.root + "/" + class_dir
      end
      ::Dir.new(class_dir).each do |file|
        next unless file =~ /\.rb$/
        path = class_dir + "/" + file
        ETL.logger.debug("Loading user job file #{path}")
        require path
      end
    end
  end
end
