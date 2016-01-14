require 'tempfile'
require 'sequel'
require 'securerandom'

module ETL::Output

  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class Sequel < Base
    attr_accessor :row_batch_size, :col_name_updated, :col_name_created

    # Initialize given a connection to the database
    def initialize(params = {})
      super
      @conn = nil
      @row_batch_size = 100
      @col_name_updated = "dw_updated"
      @col_name_created = "dw_created"
    end
    
    def conn
      @conn ||= ::Sequel.connect(@params)
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns the default schema based on the table in the destination db
    def default_schema
      return nil unless @feed_name && conn
      sequel_schema = conn.schema(@feed_name)
      ETL::Schema::Table.from_sequel_schema(sequel_schema)
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

    def value_to_db_str(col, value)
      if value.nil?
        "null"
      else
        case col.type
          when :int
            "#{value}"
          when :float
            "#{value}"
          when :numeric
            "#{value}"
          else
            "'#{value}'"
        end
      end
    end
    
    def quote_ident(ident)
      #puts("database_type: #{conn.database_type}")
      # Postgres: '"' + ident + '"'
      # Mysql: '`' + ident.to_s + '`'
      conn.quote_identifier(ident.to_s)
    end
    
    # Returns temp table name acceptable to the database constructed out
    # of the feed name and batch id
    def self.temp_table_name(feed_name, batch_id)
      # we want to keep the length < 64 chars so it works on at least Mysql
      # and PostgreSQL. Note SecureRandom returns 2 hex chars for each byte
      "#{feed_name.slice(0, 30)}_#{batch_id.slice(0, 22)}_#{SecureRandom.hex(4)}"
    end
    
    # Creates a temp table for this batch in the specified 
    # connection transaction. Returns the name of this temp table.
    def create_temp(conn)
      # Get string representation of all our columns
      type_ary = []
      schema.columns.each do |name, column|
        n = quote_ident(name)
        t = col_type_str(column)
        type_ary << "#{n} #{t}"
      end

      name = Sequel.temp_table_name(feed_name, batch_id)
      name.gsub!(/\W/, '')
      sql = "create temporary table #{name} (#{type_ary.join(', ')});"
      logger.debug(sql)
      conn.run(sql)
      return name
    end
    alias qi quote_ident

    # Load data into temp table in batches
    def load_temp_data(conn, temp_table_name)
      logger.debug("Loading temp table #{temp_table_name} in batches " + 
        "of #{@row_batch_size} rows")
      reader.each_row_batch(@row_batch_size) do |rows|
        load_temp_data_batch(conn, temp_table_name, rows)
      end
      reader.rows_processed
    end

    # Load a single batch of rows (passed in as array) into the temp table
    def load_temp_data_batch(conn, temp_table_name, input_rows)
      logger.debug("Processing batch size #{input_rows.length}")
      rows = [] # rows we're writing
      cols = {} # columns we're writing
      
      values = [] # values we're writing
      num_rows = 0
      input_rows.each do |row_in|
        num_rows += 1
        
        # Read our input row into a hash containing all schema columns
        row_out = read_input_row(row_in) 
        
        # Perform row-level transform
        row_out = transform_row(row_out)
        
        # Store the values and remember which columns we saw
        row_out.each do |name, value|
          values << value          
          cols[name] = true
        end
      end

      # build array of quoted column names
      col_names = cols.keys
      col_names.collect! { |x| quote_ident(x) }

      # all the parameters that need to be substituted      
      params = Array.new(num_rows) do
        "(" + Array.new(col_names.length, "?").join(", ") + ")"        
      end
            
      sql = <<SQL
insert into #{temp_table_name} 
  (#{col_names.join(", ")})
values #{params.join(",\n  ")}
;
SQL
      logger.debug(sql)
      logger.debug(values)
      conn.fetch(sql, *values).all
    end

    # Perform table transformations on the temporary table before final
    # load. This function is given the names of the temporary and final
    # tables but it should only modify the temp one.
    def transform_table(conn, temp_table_name, dest_table)
      [@col_name_updated, @col_name_created].each do |col_name|
        if schema.columns.has_key?(col_name)
          sql = <<SQL
update #{temp_table_name} set #{quote_ident(col_name.to_s)} = now();
SQL
          logger.debug(sql)
          result = conn.run(sql)
        end
      end
    end

    # Load temp table records into destination table, returning number of
    # affected rows
    def load_destination_table(conn, temp_table_name, dest_table)
      q_dest_table = quote_ident(dest_table)
      # build array of quoted column names
      col_names = schema.columns.keys
      col_names.collect! { |x| quote_ident(x) }
      col_name_str = col_names.join(", ")

      # delete existing rows based on load strategy
      case load_strategy
      when :update
        # don't delete anything
      when :upsert
        # don't delete anything
      when :insert_append
        # don't delete anything
      when :insert_table
        # clear out existing table
        sql = <<SQL
delete from #{q_dest_table};
SQL
        logger.debug(sql)
        conn.run(sql)
      when :insert_partition
        # clear out records for the partition associated with this batch
        clauses = @batch.keys.collect do |bn|
          name = schema.partition_columns.fetch(bn.to_s, bn).to_s
          
          unless schema.columns.has_key?(name)
            raise ETL::SchemaError, "Schema does not have partition column '#{name}'"
          end
          
          # XXX Hack that lets us handle day columns. We need to generalize the
          # config to allow specification of transformations on partition cols
          # prior to comparison
          if name.end_with?("_at")
            "date(#{quote_ident(name)}) = ?"
          else
            "#{quote_ident(name)} = ?"
          end
        end
        sql = "delete from #{q_dest_table} where " + clauses.join(" and ")
        logger.debug(sql)
        logger.debug(@batch.values)
        conn.fetch(sql, *(@batch.values)).all
      else
        raise ETL::OutputError, "Invalid load strategy '#{load_strategy}'"
      end

      # handle insert/upsert/update
      if [:update, :upsert].include?(load_strategy)        
        # Handle primary keys from schema file
        pks = schema.primary_key
        if pks.nil? or pks.empty?
          raise ETL::SchemaError, "Schema must have primary key specified for update/upsert"
        elsif not pks.is_a?(Array)
          # convert to array
          pks = [pks]
        end

        # build sql string for updating columns
        upd_cols = schema.columns.keys
        pks.each do |pk| # don't need to update pk
          upd_cols.delete(pk) 
        end
        upd_cols.delete(@col_name_created) # also don't update created

        # Update records that already exist
        # XXX Hack - need to specialize on db type until I can generalize this
        if conn.database_type.to_s == "mysql"
          sql = <<SQL
update #{q_dest_table} dest, #{temp_table_name} tmp
set #{upd_cols.collect{ |x| qx = qi(x); "dest.#{qx} = tmp.#{qx}"}.join(", ")}
where #{pks.collect{ |pk| qpk = qi(pk); "dest.#{qpk} = tmp.#{qpk}" }.join(" and ")}
;
SQL
        else
          sql = <<SQL
update #{q_dest_table}
set #{upd_cols.collect{ |x| qx = qi(x); "#{qx} = tmp.#{qx}"}.join(", ")}
from #{temp_table_name} tmp
where #{pks.collect{ |pk| qpk = qi(pk); "#{q_dest_table}.#{qpk} = tmp.#{qpk}" }.join("\n    and ")}
;
SQL
        end

        logger.debug(sql)
        conn.fetch(sql).all

        # for upsert we also insert records that don't exist yet
        if load_strategy == :upsert
          sql = <<SQL
insert into #{q_dest_table}
  (#{col_name_str})
  select #{col_names.collect{ |x| "tmp.#{x}"}.join(", ")}
  from #{temp_table_name} tmp
  left outer join #{q_dest_table} on 
    #{pks.collect{ |pk| qpk = qi(pk); "#{q_dest_table}.#{qpk} = tmp.#{qpk}" }.join("\n    and ")}
  where #{q_dest_table}.#{quote_ident(pks[0])} is null
SQL
          logger.debug(sql)
          conn.fetch(sql).all
        end

      # else this is just a standard insert of entire temp table
      else
        sql = <<SQL
insert into #{q_dest_table}
  (#{col_name_str})
  select #{col_name_str}
  from #{temp_table_name};
SQL
        logger.debug(sql)
        conn.fetch(sql).all
      end
    end
    
    # Helper function to dump out a table to stdout
    def p_table(conn, table)
      puts("--- START #{table} ---")
      conn.fetch("select * from #{table}").each do |r|
        p(r)
      end
      puts("--- END #{table} ---")
    end

    # Runs the ETL job
    def run_internal
      rows_success = rows_error = 0
      msg = ''

      # Perform all steps within a transaction
      conn.transaction do
        # Create temp table to match destination table
        temp_table_name = create_temp(conn)

        # Load data into temp table
        rows_success = load_temp_data(conn, temp_table_name)

        # Perform full table transformation on the temp table
        dest_table = feed_name
        transform_table(conn, temp_table_name, dest_table)

        # Load temp table records into destination table
        load_destination_table(conn, temp_table_name, dest_table)

        msg = "Processed #{rows_success} input rows for #{dest_table}"
      end

      # Final result
      Result.new(rows_success, rows_error, msg)
    end
  end
end
