require 'etl/util/hash_util'
require 'psych'
require 'singleton'

module ETL

  # Configuration class
  class Config
    attr_accessor :config_dir, :db

    include Singleton

    def initialize
      @config_dir = ENV['ETL_CONFIG_DIR'] || File.expand_path('../../../etc', __FILE__)
    end

    def db_file
      @config_dir + "/database.yml"
    end

    def db(&b)
      get_envvars = is_true_value(ENV.fetch('ETL_DATABASE_ENVVARS', false))
      @db ||= if get_envvars
                @db = database_env_vars
              else
                @db ||= self.class.load_file(db_file)
              end
      yield @db if block_given?
      @db
    end

    def database_env_vars
      conn_params = {}
      conn_params[:encoding] = "utf8"
      conn_params[:reconnect] = false
      conn_params[:pool] = 5
      conn_params[:adapter] = ENV.fetch('ETL_DATABASE_ADAPTER', 'postgres')
      conn_params[:dbname] = ENV.fetch('ETL_DATABASE_DB_NAME', 'postgres')
      conn_params[:username] = ENV.fetch('ETL_DATABASE_USER', 'root')
      conn_params[:password] = ENV.fetch('ETL_DATABASE_PASSWORD', 'root')
      conn_params[:host] = ENV.fetch('ETL_DATABASE_HOST', 'localhost')
      conn_params[:port] = ENV.fetch('ETL_DATABASE_PORT', 5432)
      conn_params
    end

    def aws_file
      @config_dir + "/aws.yml"
    end

    def aws(&b)
      get_envvars = ENV.fetch('ETL_AWS_ENVVARS', false)
      @aws ||= if is_true_value(get_envvars)
                @aws = {}
                @aws[:aws_region] = ENV.fetch('ETL_AWS_REGION', 'us-west-2')
                @aws[:s3_bucket] = ENV.fetch('ETL_AWS_S3_BUCKET')
                @aws[:role_arn] = ENV.fetch('ETL_AWS_ROLE_ARN')
              else
                self.class.load_file(aws_file)
              end
      yield @aws if block_given?
      @aws
    end

    def redshift_file
      @config_dir + "/redshift.yml"
    end

    def redshift(&b)
      get_envvars = is_true_value(env.fetch('etl_redshift_envvars', false))
      @redshift ||= if get_envvars
                      use_odbc_dsn_connection = is_true_value(env.fetch('etl_redshift_odbc_connection', false))
                      @redshift = {}
                      @redshift[:user] = ENV.fetch('ETL_REDSHIFT_USER', 'masteruser')
                      @redshift[:password] = ENV.fetch('ETL_REDSHIFT_PASSWORD', 'root')
                      if !use_odbc_dsn_connection
                        @redshift[:dbname] = ENV.fetch('ETL_REDSHIFT_DB_NAME', 'dev')
                        @redshift[:host] = ENV.fetch('ETL_RESHIFT_HOST')
                        @redshift[:port] = ENV.fetch('ETL_REDSHIFT_PORT', 5439)
                      else
                        @redshift[:port] = ENV.fetch('ETL_REDSHIFT_DSN', 'MyRealRedshift')
                      end
                    elsif
                    else
                      self.class.load_file(redshift_file)
                    end
      yield @redshift if block_given?
      @redshift
    end

    def influx_file
      @config_dir + "/influx.yml"
    end

    def influx(&b)
      get_envvars = is_true_value(ENV.fetch('ETL_INFLUX_ENVVARS', false))
      @influx ||= if get_envvars
                    @influx = {}
                    @influx[:password] = ENV.fetch('ETL_INFLUXDB_PASSWORD')
                    @influx[:port] = ENV.fetch('ETL_INFLUXDB_PORT', 8086)
                    @influx[:host] = ENV.fetch('ETL_INFLUXDB_HOST', 'influxdb.service.consul')
                    @influx[:database] = ENV.fetch('ETL_INFLUXDB_DB', 'metrics')
                  else
                    self.class.load_file(influx_file)
                  end
      yield @influx if block_given?
      @influx
    end

    def core_file
      @config_dir + "/core.yml"
    end

    def core(&b)
      get_envvars = is_true_value(ENV.fetch('ETL_CORE_ENVVARS', false))
      @c ||= if get_envvars
                c = {}
                c[:default] = {}
                c[:default][:class_dir] = ENV.fetch('ETL_CLASS_DIR', DIR.pwd)

                c[:job] = {}
                c[:job][:class_dir] = ENV.fetch('ETL_CLASS_DIR', DIR.pwd)
                c[:job][:data_dir] = ENV.fetch('ETL_DATA_DIR')
                c[:job][:retry_max] = 5 # max times retrying jobs
                c[:job][:retry_wait] = 4 # seconds
                c[:job][:retry_mult] = 2.0 # exponential backoff multiplier

                c[:log] = {}
                c[:class] = "ETL::Logger"
                c[:level] = ENV.fetch('ETL_LOG_LEVEL', 'debug')

                c[:database] = database_env_vars

                c[:queue] = {}
                c[:queue][:class] = ENV.fetch('ETL_QUEUE_CLASS', 'ETL::Queue::File')
                c[:queue][:path] = ENV.fetch('ETL_QUEUE_PATH', "/var/tmp/etl_queue")

                c[:metrics] = {}
                c[:metrics][:class] = ENV.fetch('ETL_METRICS_CLASS', "ETL::Metrics")
                c[:metrics][:file] = ENV.fetch('ETL_METRICS_FILE_PATH', '/tmp/etl-metrics.log')
                c[:metrics][:series] = 'etlv2_job_run'

                c[:slack] = {}
                c[:slack][:url] = ENV.fetch('ETL_SLACK_URL')
                c[:slack][:channel] = ENV.fetch('ETL_SLACK_CHANNEL')
                c[:slack][:username] = ENV.fetch('ETL_SLACK_USERNAME')
              else
                self.class.load_file(core_file)
              end
      yield @c if block_given?
      @c
    end

    # helper for env var values to ensure a string value is actually true
    def is_true_value(v) 
      if v.nil?
        return false
      elsif v == false
        return false
      elsif v.kind_of(String) && v.downcase == 'true'
        return true
      end
      return false
    end

    def self.load_file(file)
      ETL::HashUtil::symbolize_keys(Psych.load_file(file))
    end
  end

  def self.config
    Config.instance
  end
end
