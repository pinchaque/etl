libdir = File.expand_path("..", __FILE__)
$LOAD_PATH.unshift(libdir)

# Pre-define the module so we can use simpler syntax
module ETL
end

require 'sequel'

# Core classes
require 'etl/exception'

# Utilities
require 'etl/util/logger'
require 'etl/util/metrics'
require 'etl/util/hash_util'
require 'etl/util/string_util'
require 'etl/batch'

# Models
require 'etl/models/job_run'
require 'etl/models/job_run_repository'

::ETL::Model::JobRunRepository.instance = ::ETL::Model::JobRunRepository.new

require 'etl/schema/table'
require 'etl/schema/column'

require 'etl/job/result'
require 'etl/job/base'
require 'etl/job/manager'

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
  # creates aws credentials to log
  def self.create_aws_credentials(region, iam_role, session_name)
    if ENV["TEST_AWS_ACCESS_KEY_ID"].nil?
      sts = Aws::STS::Client.new(region: region)
      session = sts.assume_role(
        role_arn: iam_role,
        role_session_name: session_name
      )

      creds = Aws::Credentials.new(
        session.credentials.access_key_id,
        session.credentials.secret_access_key,
        session.credentials.session_token
      )
    else
      # Note this branch of code is really for testing purposes
      # when running from a machine that is not an ec2 instance
      # which is why TEST is affixed ahead of it
      creds = Aws::Credentials.new(
         ENV["TEST_AWS_ACCESS_KEY_ID"],
         ENV["TEST_AWS_SECRET_ACCESS_KEY"]
      )
    end
  end

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

  def ETL.create_metrics
    ETL.create_class(:metrics)
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
    class_dirs_map = {}
    if c = ETL.config.core.fetch(:default, {})[:class_dir]
      find_dirs(c, class_dirs_map)
    end
    if c = ETL.config.core[:job][:class_dir]
      find_dirs(c, class_dirs_map)
    end
    class_dirs_map.keys.each do |c|
      load_class_dir(c)
    end
  end

  private

  # loading the sub directories of the supplied base directory. Adding dirs to hash in case there are duplicates to remove them
  def ETL.find_dirs(dir, dirs_map)
      Dir.entries(dir).select {|entry| File.directory? File.join(dir ,entry) and !(entry =='.' || entry == '..') }.each  do | f|
        dirs_map["#{dir}/#{f}"] = true
      end
      dirs_map[dir] = true
  end

  # Function to load external classes in the specified directory
  def ETL.load_class_dir(class_dir)
    unless class_dir.start_with?("/")
      class_dir = ETL.root + "/" + class_dir
    end
    ::Dir.new(class_dir).each do |file|
      next unless file =~ /\.rb$/
      path = class_dir + "/" + file
      ETL.logger.debug("Loading user file #{path}")
      require path
    end
  end
end
