require 'sequel'
require 'odbc'
require 'mixins/cached_logger'

module ETL::Redshift

  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME="Amazon Redshift (x64)"

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :db
    def initialize(conn_params={})
      @conn_params = conn_params
      ObjectSpace.define_finalizer(self, proc { db.disconnect })
    end

    def db
      @db ||= begin
                Sequel.odbc(@conn_params)
              end
    end

    def execute(sql)
      log.debug("SQL: '#{sql}'")
      db.execute(sql)
    end

    def fetch(sql)
      log.debug("SQL: '#{sql}'")
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
