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
require 'pg'
require 'securerandom'

module ETL::Job


  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class RelationalDB < Base
    attr_accessor :row_batch_size

    # Initialize given a connection to the database
    def initialize(input, conn)
      super(input)
      @conn = conn
      @row_batch_size = 100
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns string that can be used as the database type given the 
    # ETL::Schema::Column object
    def col_type_str(col)
      case col.type
        when :string
          "varchar"
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

    # Creates a temp table for this batch in the specified 
    # connection transaction. Returns the name of this temp table.
    def create_temp(conn)
      # Get string representation of all our columns
      type_ary = []
      schema.columns.each do |name, column|
        n = conn.quote_ident(name)
        t = conn.escape_string(col_type_str(column))
        type_ary << "#{n} #{t}"
      end

      temp_table_name = "#{feed_name}_#{@batch.to_s}_#{SecureRandom.hex(8)}"
      temp_table_name.gsub!(/\W/, '')
      temp_table_name = conn.quote_ident(temp_table_name)
      sql = "create temp table #{temp_table_name} (#{type_ary.join(', ')});"
      logger.debug(sql)
      conn.exec(sql)
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
      input_rows.each do |row_in|
        # Read our input row into a hash containing all schema columns
        row_out = read_input_row(row_in) 
        
        # Perform row-level transform
        row_out = transform_row(row_out)
        
        # Now we need to put each value into the SQL string in a format
        # PG will recognize. That means single quotes around strings, etc.
        # To do this we rely on the column types.

        # Convert each value to its string representation
        str_values = []
        row_out.each do |name, value|
          cols[name] = true
          type = schema.columns[name]
          esc_string = value.is_a?(String) ? conn.escape_string(value) : value
          str_values << value_to_db_str(type, esc_string)
        end

        # string representation of values for this row
        rows << "(" + str_values.join(", ") + ")"
      end

      # build array of quoted column names
      col_names = cols.keys
      col_names.collect! { |x| conn.quote_ident(x) }

      sql = <<SQL
insert into #{temp_table_name} 
  (#{col_names.join(", ")})
values #{rows.join(",\n  ")}
;
SQL
      logger.debug(sql)
      conn.exec(sql)
    end

    # Load temp table records into destination table, returning number of
    # affected rows
    def load_destination_table(conn, temp_table_name, dest_table)
      # build array of quoted column names
      col_names = schema.columns.keys
      col_names.collect! { |x| conn.quote_ident(x) }
      col_name_str = col_names.join(", ")

      sql = ""

      # delete existing rows based on load strategy
      case load_strategy
      when :insert_append
        # don't delete anything
      when :insert_table
        # clear out existing table
        sql += <<SQL
delete from #{conn.quote_ident(dest_table)};
SQL
      else
        raise "Invalid load strategy '#{load_strategy}'"
      end

      sql += <<SQL
insert into #{conn.quote_ident(dest_table)}
  (#{col_name_str})
  select #{col_name_str}
  from #{temp_table_name};
SQL
      logger.debug(sql)
      result = conn.exec(sql)

      # return number of rows affected
      result.cmd_tuples
    end

    # Runs the ETL job
    def run_internal

      rows_success = rows_error = 0
      msg = ''

      # Perform all steps within a transaction
      @conn.transaction do |conn|

        # Create temp table to match destination table
        temp_table_name = create_temp(conn)

        # Load data into temp table
        load_temp_data(conn, temp_table_name)

        # Load temp table records into destination table
        dest_table = feed_name
        rows_success = load_destination_table(conn, temp_table_name, dest_table)
        msg = "Wrote #{rows_success} rows to #{dest_table}"
      end

      # Final result
      Result.new(rows_success, rows_error, msg)
    end
  end
end
