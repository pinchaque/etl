module ETL
  # The root directory of the etl code tree
  def ETL.root
    File.expand_path('../..', __FILE__)
  end
  
  # Loads in the remaining ETL-related files. This should be called after
  # the ETL system is configured.
  def ETL.bootstrap
    # Include the rest of code needed for ETL system
    require 'etl/core'
  end
end

# Process our configuration files
require 'etl/config'
