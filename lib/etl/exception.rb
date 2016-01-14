module ETL
  # Exception in processing input data
  class InputError < ::RuntimeError
  end
  
  # Exception when writing to output from input
  class OutputError < ::RuntimeError
  end
  
  class JobError < ::RuntimeError
  end
end
