require 'sequel'
require 'odbc'
require 'mixins/cached_logger'

module ETL::Redshift

  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME="MyRedshiftDriver"

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :driver, :server, :port, :username, :password
    def initialize(conn_params={})
      @driver = conn_params.fetch(:driver, REDSHIFT_ODBC_DRIVER_NAME)
      @server = conn_params.fetch(:host, "localhost")
      @db_name = conn_params.fetch(:database, "dev")
      @port =  conn_params.fetch(:port, 5439)
      @password = conn_params.fetch(:password, '')
      @user = conn_params.fetch(:user, "masteruser")
      ObjectSpace.define_finalizer(self, proc { db.disconnect })
    end

    def db
      @db ||= begin
                conn_str = "Driver={#{@driver}}; Servername=#{@server}; Database=#{@db_name}; UID=#{@user}; PWD=#{@password}; Port=#{@port}"
                log.debug("ODBC Connection String: #{conn_str}")
                Sequel.odbc(:drvconnect=> conn_str)
              end
    end

    def execute(sql)
      log.debug(sql)
      db.execute(sql)
    end

    def fetch(sql)
      log.debug(sql)
      db.fetch(sql)
    end

    def drop_table(table_name)
      sql = "drop table if exists #{table_name};"
      execute(sql)
    end

    def create_table(table)
      sql = table.create_table_sql
      execute(sql)
    end

    def columns(table_name)
      sql = <<SQL
      SELECT "column", type FROM pg_table_def WHERE tablename = '#{table_name}'
SQL
      fetch(sql).all
    end

  end

end
