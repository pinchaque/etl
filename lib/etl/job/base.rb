require 'mixins/cached_logger'

module ETL::Job

  # Base class for all jobs that are run
  class Base
    include ETL::CachedLogger
    
    attr_reader :batch
    
    def initialize(b)
      @batch = b
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
    
    # By default we use the class name for the ID so we can instantiate the 
    # class again later from this ID.
    def id
      self.class.name
    end
    
    def to_s
      "#{id}<#{@batch ? @batch.to_s : "NIL BATCH"}>"
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
          batch: @batch.to_s,
      }
    end
  end
end
