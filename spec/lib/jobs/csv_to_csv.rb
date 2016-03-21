
module ETL::Test
  class CsvToCsvJob < ETL::Job::Base
    def input_class
      ETL::Input::Null
    end
    
    def output_class
      ETL::Output::Null
    end
    
    def batch_factory_class
      ETL::BatchFactory::Day
    end
    
    def output_params
      { success: 34, error: 1, message: 'congrats!', sleep: nil, exception: nil }
    end
  end
end
