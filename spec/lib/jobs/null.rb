module ETL::Test
  class Null < ETL::Job::Base
    register_job
    
    def output_params
      { success: 42, error: 0, message: 'Null Job' }
    end
  end
end
