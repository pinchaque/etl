require 'tempfile'
require 'aws-sdk'
require 'pg'

module ETL::Output

  # Class that contains shared logic for loading data from S3 to Redshift.
  class Redshift < Base
    attr_accessor :load_strategy, :conn_params, :aws_params, :dest_table

    def initialize(load_strategy, conn_params={}, aws_params={})
      super()

      @aws_params = aws_params
      Aws.config.update({
        region: @aws_params[:region],
        credentials: Aws::Credentials.new(
          @aws_params[:access_key_ID],
          @aws_params[:secret_access_key])
      })
      @load_strategy = load_strategy
      @conn = nil
      @conn_params = conn_params
      @bucket = @aws_params[:s3_bucket] 
    end

    def conn
      @conn ||= PG.connect(@conn_params)
    end

    # Name of the destination table. By default we assume this is the class
    # name but you can override this in the parameters.
    def dest_table
      @dest_table || 
        ETL::StringUtil::camel_to_snake(ETL::StringUtil::base_class_name(self.class.name))
    end

    def staging_table
      "staging_"+dest_table
    end

    # Returns the default schema based on the table in the destination db
    def default_schema
      return nil unless dest_table && conn
      sql = <<SQL
      SELECT "column", type, distkey, sortkey FROM pg_table_def WHERE tablename = '#{dest_table}'
SQL
      log.debug(sql)
      redshift_schema = conn.exec(sql)

      ETL::Schema::Table.from_redshift_schema(redshift_schema.values)
    end

    # create dest table if it doesn't exist
    def create_table
      type_ary = []
      schema.columns.each do |name, column|
        t = col_type_str(column)
        type_ary << "#{name} #{t}"
      end

      sql = <<SQL
      CREATE TABLE IF NOT EXISTS #{dest_table} (#{type_ary.join(', ')})
SQL
      log.debug(sql)
      conn.exec(sql)
    end

    # Returns string that can be used as the database type given the 
    # ETL::Schema::Column object
    def col_type_str(col)
      case col.type
        when :string
          "varchar(255)"
        when :date
          "timestamp"
        when :numeric
          s = "numeric"
          if not col.width.nil? or not col.precision.nil?
            s += "("
            s += col.width.nil? ? "0" : col.width.to_s()
            if not col.precision.nil?
              s += ", #{col.precision}"
            end
            s += ")"
          end
          s
        else
          # Allow other types to just flow through, which gives us a simple
          # way of supporting columns that are coming in through db reflection
          # even if we don't know what they are.
          col.type.to_s
      end
    end

    def create_staging_table
      sql = <<SQL
        CREATE TABLE #{staging_table} (like #{dest_table})
SQL
      log.debug(sql)
      conn.exec(sql)

      sql =<<SQL
        COPY #{staging_table}
        FROM 's3://#{@bucket}/#{dest_table}'
        CREDENTIALS 'aws_iam_role=#{@aws_params[:iam]}'
        DELIMITER ','
        IGNOREHEADER 1 
        REGION '#{@aws_params[:region]}'
SQL

      log.debug(sql)
      conn.exec(sql)
    end

    def drop_staging_table
      sql =<<SQL
     DROP TABLE #{staging_table}
SQL
      log.debug(sql)
      conn.exec(sql)
    end

    def load_fromS3(conn)
      # delete existing rows based on load strategy
      case @load_strategy
      when :update
        # don't delete anything
      when :upsert
        # don't delete anything
      when :insert_append
        # don't delete anything
      when :insert_table
        # clear out existing table
        sql = <<SQL
delete from #{dest_table};
SQL
        log.debug(sql)
        conn.exec(sql)
      else
        raise ETL::OutputError, "Invalid load strategy '#{load_strategy}'"
      end

      # handle upsert/update
      if [:update, :upsert].include?(@load_strategy)   
        #get_primarykey
        pks = schema.primary_key 

        if pks.nil? or pks.empty?
          raise ETL::SchemaErorr, "Table '#{dest_table}' does not have a primary key"
        elsif not pks.is_a?(Array)
          # convert to array
          pks = [pks]
        end

        #create staging table
        create_staging_table

        sql = <<SQL
        select * from #{staging_table}
SQL

        r = conn.exec(sql)

        if @load_strategy == :upsert      
          sql = <<SQL
          DELETE FROM #{dest_table}
          USING #{staging_table} s
          WHERE #{pks.collect{ |pk| "#{dest_table}.#{pk} = s.#{pk}" }.join(" and ")}
SQL
          log.debug(sql)
          conn.exec(sql)

          sql = <<SQL
          INSERT INTO #{dest_table}
          SELECT * FROM #{staging_table}
SQL
          log.debug(sql)
          conn.exec(sql)

        # handle upsert(primary key is required)
        elsif @load_strategy == :update     
          #build query string for updating columns
          update_cols = schema.columns.keys
          pks.each do |pk|
            update_cols.delete(pk)
          end

          sql = <<SQL
  update #{dest_table}
  set #{update_cols.collect{ |x| "\"#{x}\" = s.#{x}"}.join(", ")}
  from #{staging_table} s
  where #{pks.collect{ |pk| "#{dest_table}.#{pk} = s.#{pk}" }.join(" and ")}
SQL

          log.debug(sql)
          conn.exec(sql)
        end

        #drop staging table
        drop_staging_table

      else
        sql = <<SQL
        COPY #{@dest_table}
        FROM 's3://#{@bucket}/#{@dest_table}'
        CREDENTIALS 'aws_iam_role=#{@aws_params[:iam]}'
        DELIMITER ','
        IGNOREHEADER 1 
        REGION '#{@aws_params[:region]}'
SQL
        log.debug(sql)
        conn.exec(sql)
      end
    end

     # Runs the ETL job
    def run_internal
      rows_processed = 0
      msg = ''

      # Perform all steps within a transaction
      conn.transaction do
        # create destination table if it doesn't exist
        create_table

        # Load s3 data into destination table
        load_fromS3(conn)

        msg = "Processed #{rows_processed} input rows for #{dest_table}"
      end

      ETL::Job::Result.success(rows_processed, msg)
    end
  end
end

