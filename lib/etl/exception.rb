module ETL
  # Exception in processing input data
  class InputError < ::RuntimeError
  end
  
  # Exception when running ETL job
  class JobError < ::RuntimeError
  end
end
