require 'sequel'
# removing due to ubuntu 14.04 deployment issues
#require 'odbc'
require 'mixins/cached_logger'
require 'pg'

module ETL::Redshift

  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME="Amazon Redshift (x64)"

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :db

    # when odbc driver is fully working the use redshift driver can
    # default to true
    def initialize(conn_params={})
      @use_redshift_odbc_driver = false
      @conn_params = conn_params
      ObjectSpace.define_finalizer(self, proc { db.disconnect })
    end

    def db
      @db ||= begin
                puts @conn_params.inspect
                PG.connect(@conn_params)
# removing due to ubuntu 14.04 deployment issues
#                if @use_redshift_odbc_driver then
#                  Sequel.odbc(@conn_params)
#                else
#                  Sequel.postgres(@conn_params)
#                end
              end
    end

    def execute(sql)
      log.debug("SQL: '#{sql}'")
      db.exec(sql)
    end

    def drop_table(table_name)
      sql = "drop table if exists #{table_name};"
      execute(sql)
    end

    def create_table(table)
      sql = table.create_table_sql(@use_redshift_odbc_driver)
      execute(sql)
    end

    def columns(table_name)
      sql = <<SQL
      SELECT "column", type FROM pg_table_def WHERE tablename = '#{table_name}'
SQL
      execute(sql)
    end

  end
end
