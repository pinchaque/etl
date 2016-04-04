module ETL
  # Exception in processing input data
  class InputError < ::RuntimeError
  end
  
  # Exception when writing to output from input
  class OutputError < ::RuntimeError
  end
  
  # Exception while running job
  class JobError < ::RuntimeError
  end
  
  # Exception while parsing or validating batches
  class BatchError < ::RuntimeError
  end
end
