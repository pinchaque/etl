require 'etl/jobs/file.rb'

module ETL::Job

  class CSV < File

    def run_internal(batch_id)

      # get schema 
      # create temp space using schema
      # read input
      # validate temp file
      # prepare final destination
      # load data into final destination
      # commit
      
    end

  end
end
