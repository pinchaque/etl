require 'mixins/cached_logger'

module ETL::Job

  # Base class for all jobs that are run
  class Base
    include ETL::CachedLogger
    
    attr_reader :batch
    
    def initialize(b)
      @batch = b
    end
    
    # Registers a job class with the manager. This is typically called by
    # subclasses to register themselves with a convenient id to represent
    # that subclass. Only registered jobs can be executed.
    def self.register_job
      Manager.instance.register(id, self)
    end
    
    # Run the job by instantiating input and output classes with parameters
    # and then running the output class for this batch
    def run
      log.debug("Input: #{self.class.input_class.name} #{input_params}")
      log.debug("Output: #{self.class.output_class.name} #{output_params}")
      log.debug("Batch: #{@batch.to_s}")
      input = self.class.input_class.new(input_params)
      input.log = log
      output = self.class.output_class.new(output_params)
      output.log = log
      output.reader = input
      output.batch = @batch
      output.run
    end
    
    # Default class function for getting ID based on class name
    def self.id
      ETL::StringUtil::camel_to_snake(ETL::StringUtil::base_class_name(name.to_s))
    end

    # Default function for getting ID based on class name
    def id
      self.class.id
    end
    
    def to_s
      batch_id = (@batch && @batch.id) || "NO_BATCH"
      "#{id}<#{batch_id}>"
    end
    
    # Instantiates schedule for this job
    def schedule
      @schedule ||= self.class.schedule_class.new(self, @batch)
    end
  
    protected
    def self.schedule_class
      ETL::Schedule::Never
    end
    
    def self.batch_factory_class
      ETL::BatchFactory::Base
    end
  
    def self.input_class
      ETL::Input::Null
    end
    
    def input_params
      {}
    end
    
    def self.output_class
      ETL::Output::Null
    end
    
    def output_params
      {}
    end

    def log_context
      {
          job: id.to_s,
          batch: (@batch && @batch.id) || "nil",
      }
    end
  end
end
