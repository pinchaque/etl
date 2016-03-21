require 'mixins/cached_logger'

module ETL::Job

  # Base class for all jobs that are run
  class Base
    include ETL::CachedLogger
    
    def initialize(batch)
      @batch = batch
    end
    
    # Run the job by instantiating input and output classes with parameters
    # and then running the output class for this batch
    def run
      log.debug("Input: #{input_class.name} #{input_params}")
      log.debug("Output: #{output_class.name} #{output_params}")
      log.debug("Batch: #{@batch.to_s}")
      input = input_class.new(input_params)
      input.log = log
      output = output_class.new(output_params)
      output.log = log
      output.reader = input
      output.batch = @batch
      output.run
    end
  
    protected
    def batch_factory_class
      ETL::BatchFactory::Base
    end
  
    def input_class
      ETL::Input::Null
    end
    
    def input_params
      {}
    end
    
    def output_class
      ETL::Output::Null
    end
    
    def output_params
      {}
    end

    def log_context
      {
          job: self.class.name.gsub(/^.*::/, ''),
          batch: @batch.to_s,
      }
    end
  end
end
