require 'tempfile'
require 'aws-sdk'
require 'pg'
require 'csv'

module ETL::Output

  # Class that contains shared logic for loading data from S3 to Redshift.
  class Redshift < Base
    attr_accessor :load_strategy, :conn_params, :aws_params, :dest_table

    def initialize(load_strategy, conn_params={}, aws_params={}, header=nil)
      super()

      @aws_params = aws_params
      @load_strategy = load_strategy
      @conn = nil
      @conn_params = conn_params
      @bucket = @aws_params[:s3_bucket]
      @random_key = [*('a'..'z'),*('0'..'9')].shuffle[0,10].join
      @header = header
    end

    def csv_file 
      @csv_file ||= Tempfile.new(dest_table) 
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

    def tmp_table
      dest_table+"_"+@random_key
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
        type_ary << "\"#{name}\" #{t}"
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
          if !col.width.nil? || !col.precision.nil?
            s += "("
            s += col.width.nil? ? "0" : col.width.to_s()
            if !col.precision.nil?
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
        CREATE TEMP TABLE #{tmp_table} (like #{dest_table})
SQL
      log.debug(sql)
      conn.exec(sql)

      sql =<<SQL
        COPY #{tmp_table}
        FROM 's3://#{@bucket}/#{tmp_table}'
        IAM_ROLE '#{@aws_params[:role_arn]}'
        TIMEFORMAT AS 'auto'
        DELIMITER ','
        REGION '#{@aws_params[:region]}'
SQL

      log.debug(sql)
      conn.exec(sql)
    end

    def creds
      sts = Aws::STS::Client.new(region: @aws_params[:region])
      if tmp_table.length > 50
        session = sts.assume_role(
          role_arn: @aws_params[:role_arn],
          role_session_name: "circle-#{tmp_table[0..49]}-upload"
        )
      else
        session = sts.assume_role(
          role_arn: @aws_params[:role_arn],
          role_session_name: "circle-#{tmp_table}-upload"
        )
      end

      Aws::Credentials.new(
        session.credentials.access_key_id,
        session.credentials.secret_access_key,
        session.credentials.session_token
      )
    end

    def upload_to_s3
      s3_resource = Aws::S3::Resource.new(region: @aws_params[:region], credentials: creds)
      s3_resource.bucket(@bucket).object(tmp_table).upload_file(csv_file.path)
    end

    def delete_object_from_s3
      s3_client = Aws::S3::Client.new(region: @aws_params[:region], credentials: creds)
      s3_response = s3_client.delete_object({
        bucket: @bucket,
        key: tmp_table
      })
    end

    def load_from_s3
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

        if pks.nil? || pks.empty?
          raise ETL::SchemaError, "Table '#{dest_table}' does not have a primary key"
        elsif !pks.is_a?(Array)
          # convert to array
          pks = [pks]
        end

        #create staging table
        create_staging_table

        sql = <<SQL
        select * from #{tmp_table}
SQL

        r = conn.exec(sql)

        if @load_strategy == :upsert
          sql = <<SQL
          DELETE FROM #{dest_table}
          USING #{tmp_table} s
          WHERE #{pks.collect{ |pk| "#{dest_table}.#{pk} = s.#{pk}" }.join(" and ")}
SQL
          log.debug(sql)
          conn.exec(sql)

          sql = <<SQL
          INSERT INTO #{dest_table}
          SELECT * FROM #{tmp_table}
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
  from #{tmp_table} s
  where #{pks.collect{ |pk| "#{dest_table}.#{pk} = s.#{pk}" }.join(" and ")}
SQL

          log.debug(sql)
          conn.exec(sql)
        end

      else
        sql = <<SQL
        COPY #{@dest_table}
        FROM 's3://#{@bucket}/#{tmp_table}'
        IAM_ROLE '#{@aws_params[:role_arn]}'
        TIMEFORMAT AS 'auto'
        DELIMITER ','
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

        # Load data into temp csv
        ::CSV.open(csv_file.path, "w") do |c|
          reader.each_row do |row|
            if !@header.nil?
              s = @header.map{|k| row[k.to_s]}
              if !s.nil?
                c << s
                rows_processed += 1
              end
            else
              if !row.nil?
                c << row.values
                rows_processed += 1
              end
            end
          end
        end
       
        if rows_processed > 0
          #To-do: load data into S3
          upload_to_s3

          # Load s3 data into destination table
          load_from_s3

          # delete s3 data
          delete_object_from_s3
        end

        msg = "Processed #{rows_processed} input rows for #{dest_table}"
      end

      ETL::Job::Result.success(rows_processed, msg)
    end
  end
end
