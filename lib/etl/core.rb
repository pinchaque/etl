# Pre-define the module so we can use simpler syntax
module ETL
end

require 'sequel'

# Core classes
require 'etl/exception'

# Utilities
require 'etl/util/logger'
require 'etl/util/hash_util'
require 'etl/util/string_util'
require 'etl/batch'

# Models
# Set up the database connection that's needed for Sequel models
# Also we can use the DB constant in the rest of the code
Sequel::Model.db = Sequel.connect(ETL.config.core[:database])
Sequel::Model.plugin :timestamps
require 'etl/models/job_run'

require 'etl/schema/table'
require 'etl/schema/column'

require 'etl/job/result'
require 'etl/job/base'
require 'etl/job/manager'

libdir = File.expand_path("..", __FILE__)
base_file = 'base.rb'
%w( input output transform queue batch_factory schedule ).each do |d|
  dir = "#{libdir}/#{d}"
  require "#{dir}/#{base_file}"
  Dir.new(dir).each do |file|
    next unless file =~ /\.rb$/
    next if file == base_file
    require "#{dir}/#{file}"
  end
end

module ETL
  
  # Generic App-wide logger
  def ETL.logger
    @@logger ||= ETL.create_logger
  end
  
  # Sets generic App-wide logger
  def ETL.logger=(v)
    @@logger = v
  end
  
  # Creates a new logger instance that we can use for different contexts 
  # based on context that is passed in
  def ETL.create_logger(context = {})
    log = ETL.create_class(:log)
    log.context = context.dup
    log
  end
  
  def ETL.queue
    @@queue ||= ETL.create_queue
  end
  
  def ETL.queue=(v)
    @@queue = v
  end
  
  def ETL.create_queue
    ETL.create_class(:queue)
  end  
  
  # Helper function to create a class given a class name stored in the config
  # under "sym"
  def ETL.create_class(sym)
    cfg = ETL.config.core[sym]
    Object::const_get(cfg[:class]).new(cfg)
  end
  
  # load all user job classes
  def ETL.load_user_classes
    if c = ETL.config.core[:job][:class_dir]
      ETL::Job::Manager.load_job_classes(c)
    end
  end
end  
