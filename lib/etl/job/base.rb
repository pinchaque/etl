module ETL::Job

  # Base class for all jobs that are run
  class Base
    def initialize(batch)
      @batch = batch
    end
    
    # Run the job by instantiating input and output classes with parameters
    # and then running the output class for this batch
    def run
      input = input_class.new(input_params)
      output = output_class.new(output_params)
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

    def log
      ETL.logger
    end
  end
end
