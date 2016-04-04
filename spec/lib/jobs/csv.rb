module ETL::Test
  class Csv < ETL::Job::Base
    register_job
    
    class Input < ETL::Input::CSV
    end
    
    def input_class
      Input
    end
    
    class Output < ETL::Output::CSV
    end
    
    def output_class
      Output
    end
    
    def batch_factory_class
      ETL::BatchFactory::Hour
    end
    
    def output_params
      { success: 34, error: 1, message: 'congrats!', sleep: nil, exception: nil }
    end
  end
end
