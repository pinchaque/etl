###############################################################################
# Copyright (C) 2015 Chuck Smith
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
###############################################################################

require 'tempfile'
require 'sequel'
require 'securerandom'

module ETL::Job


  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class Sequel < Base
    attr_accessor :row_batch_size, :col_name_updated, :col_name_created

    # Initialize given a connection to the database
    def initialize(input, conn)
      @conn = conn
      @row_batch_size = 100
      @col_name_updated = "dw_updated"
      @col_name_created = "dw_created"
      super(input)
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns the default schema based on the table in the destination db
    def default_schema
      return nil unless @feed_name
      sequel_schema = @conn.schema(@feed_name)
      ETL::Schema::Table.from_sequel_schema(sequel_schema)
    end

    # Returns string that can be used as the database type given the 
    # ETL::Schema::Column object
    def col_type_str(col)
      case col.type
        when :string
          "varchar(1024)"
        when :date
          "timestamp"
        when :int
          "int"
        when :float
          "float"
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
          "unknown"
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
      #XXX postgres supports quoting but not mysql so we need to generalize this
      #'"' + ident + '"'
      ident
    end
    
    # Creates a temp table for this batch in the specified 
    # connection transaction. Returns the name of this temp table.
    def create_temp(conn)
      # Get string representation of all our columns
      type_ary = []
      schema.columns.each do |name, column|
        n = quote_ident(name)
        t = col_type_str(column)
        type_ary << "#{name} #{t}"
      end

      temp_table_name = "#{feed_name}_#{batch_id}_#{SecureRandom.hex(8)}"
      temp_table_name.gsub!(/\W/, '')
      sql = "create temporary table #{temp_table_name} (#{type_ary.join(', ')});"
      logger.debug(sql)
      conn.run(sql)
      return temp_table_name
    end

    # Load data into temp table in batches
    def load_temp_data(conn, temp_table_name)
      logger.debug("Loading temp table #{temp_table_name} in batches " + 
        "of #{@row_batch_size} rows")
      reader.each_row_batch(@row_batch_size) do |rows|
        load_temp_data_batch(conn, temp_table_name, rows)
      end
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
      rows_changed = 0
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
delete from #{dest_table};
SQL
        logger.debug(sql)
        conn.run(sql)
      when :insert_partition
        # clear out records for the partition associated with this batch
        clauses = @batch.keys.collect do |bn|
          name = schema.partition_columns.fetch(bn.to_s, bn).to_s
          
          unless schema.columns.has_key?(name)
            raise "Schema does not have partition column '#{name}'"
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
        sql = "delete from #{dest_table} where " + clauses.join(" and ")
        logger.debug(sql)
        logger.debug(@batch.values)
        conn.fetch(sql, *(@batch.values)).all
      else
        raise "Invalid load strategy '#{load_strategy}'"
      end

      # handle insert/upsert/update
      if [:update, :upsert].include?(load_strategy)
        pk = schema.primary_key.to_s
        if pk.nil? or pk.empty?
          raise "Schema must have primary key specified for update/upsert"
        end
        q_pk = quote_ident(pk)

        # build sql string for updating columns
        update_cols = schema.columns.keys
        update_cols.delete(pk) # don't need to update pk
        update_cols.delete(@col_name_created) # also don't update created
        update_cols.collect! do |x|
          q_x = quote_ident(x)
          "#{q_x} = #{temp_table_name}.#{q_x}"
        end

        # Update records that already exist
        sql = <<SQL
update #{dest_table}
set #{update_cols.join(", ")}
from #{temp_table_name}
where #{temp_table_name}.#{q_pk} = #{dest_table}.#{q_pk}
;
SQL
        logger.debug(sql)
        conn.fetch(sql).all
        rows_changed += 0

        # for upsert we also insert records that don't exist yet
        if load_strategy == :upsert
          sql = <<SQL
insert into #{dest_table}
  (#{col_name_str})
  select #{col_name_str}
  from #{temp_table_name}
  where #{q_pk} not in (
    select #{q_pk} from #{dest_table}
  );
SQL
          logger.debug(sql)
          conn.fetch(sql).all
          rows_changed += 0
        end
        rows_changed

      # else this is just a standard insert of entire temp table
      else
        sql = <<SQL
insert into #{dest_table}
  (#{col_name_str})
  select #{col_name_str}
  from #{temp_table_name};
SQL
        logger.debug(sql)
        conn.fetch(sql).all
        rows_changed += 0
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
      @conn.transaction do
        # Create temp table to match destination table
        temp_table_name = create_temp(@conn)

        # Load data into temp table
        load_temp_data(@conn, temp_table_name)

        # Perform full table transformation on the temp table
        dest_table = feed_name
        transform_table(@conn, temp_table_name, dest_table)

        # Load temp table records into destination table
        rows_success = load_destination_table(@conn, temp_table_name, dest_table)

        msg = "Wrote #{rows_success} rows to #{dest_table}"
      end

      # Final result
      Result.new(rows_success, rows_error, msg)
    end
  end
end
