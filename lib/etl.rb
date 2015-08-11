require 'psych'
require 'sequel'

ENV["ETL_ENV"] ||= 'development'

module ETL
  def ETL.root
    File.expand_path('../..', __FILE__)
  end
  
  def ETL.log_file
    "#{ETL.root}/log/#{ENV['ETL_ENV']}.log"
  end
  
  # returns array of DBs parsed from config file
  def ETL.db_config
    if ETL.respond_to?("db_config_file")
      fname = ETL.db_config_file
    else
      fname = "#{ETL.root}/config/database.yml"
    end
    Psych.load_file(fname)
  end
  
  # Returns whole-app configuration hash
  def ETL.app_config
    if ETL.respond_to?("app_config_file")
      fname = ETL.app_config_file
    else
      fname = "#{ETL.root}/config/app.yml"
    end
    Psych.load_file(fname)
  end
end

# Set up the database connection that's needed for Sequel models
# Also we can use the DB constant in the rest of the code
dbconfig = ETL.db_config[ENV["ETL_ENV"]]
DB = Sequel::Model.db = Sequel.connect(dbconfig)
    
# Now include the rest of code needed for ETL system
require 'etl/core'
