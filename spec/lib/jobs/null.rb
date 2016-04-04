module ETL::Test
  class Null < ETL::Job::Base
    register_job
    
    def input_class
      ETL::Input::Null
    end
    
    def output_class
      ETL::Output::Null
    end
    
    def batch_factory_class
      ETL::BatchFactory::Null
    end
    
    def output_params
      { success: 42, error: 0, message: 'Null Job' }
    end
  end
end
