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
    attr_accessor :db, :region, :iam_role

    # when odbc driver is fully working the use redshift driver can
    # default to true
    def initialize(conn_params={})
      @use_redshift_odbc_driver = false
      @conn_params = conn_params
      ObjectSpace.define_finalizer(self, proc { db.disconnect })
    end

    def db
      @db ||= begin
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

    def count_row_by_s3(destination)
      sql = <<SQL
        SELECT c.lines_scanned FROM stl_load_commits c, stl_query q WHERE filename LIKE 's3://#{destination}%' 
        AND c.query = q.query AND trim(q.querytxt) NOT LIKE 'COPY ANALYZE%'
SQL
      results = execute(sql)
      loaded_rows = 0
      results.each { |result| loaded_rows += result.fetch("lines_scanned", "0").to_i }
      loaded_rows
    end

    def unload_to_s3(query, destination, delimiter = '|')
      sql = <<SQL 
        UNLOAD ('#{query}') TO 's3://#{destination}'
        IAM_ROLE '#{@iam_role}'
        DELIMITER '#{delimiter}'
SQL
      execute(sql)
    end

    def copy_from_s3(table_name, destination, delimiter = '|')
      sql = <<SQL
        COPY #{table_name}
        FROM 's3://#{destination}' 
        IAM_ROLE '#{@iam_role}'
        TIMEFORMAT AS 'auto'
        DATEFORMAT AS 'auto'
        ESCAPE
        DELIMITER '#{delimiter}'
        REGION '#{@region}'
SQL
      execute(sql)
    end

    def creds(session_name)
      sts = Aws::STS::Client.new(region: @region)
      session = sts.assume_role(
        role_arn: @iam_role,
        role_session_name: session_name 
      )

      Aws::Credentials.new(
        session.credentials.access_key_id,
        session.credentials.secret_access_key,
        session.credentials.session_token
      )
    end

    def delete_object_from_s3(bucket, prefix, session_name)
      s3 = Aws::S3::Client.new(region: @region, credentials: creds(session_name))
      resp = s3.list_objects(bucket: bucket)
      keys = resp[:contents].select { |content| content.key.start_with? prefix }.map { |content| content.key }

      keys.each { |key| s3.delete_object(bucket: bucket, key: key) }
    end
  end
end
