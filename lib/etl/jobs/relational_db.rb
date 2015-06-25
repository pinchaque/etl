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
require 'fileutils'
require 'csv'
require 'pg'
require 'securerandom'

module ETL::Job


  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class RelationalDB < Base

    # Initialize given a connection to the database
    def initialize(input, conn)
      super(input)
      @conn = conn
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns string that can be used as the database type given the 
    # ETL::Schema::Type object
    def col_type_str(type)
      case type.type
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
          if not type.width.nil? or not type.precision.nil?
            s += "("
            s += type.width.nil? ? "0" : type.width.to_s()
            if not type.precision.nil?
              s += ", #{type.precision}"
            end
            s += ")"
          end
          s
        else
          "unknown"
      end
    end

    def value_to_db_str(type, value)
      case type.type
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
      schema.columns.each do |colname, coltype|
        n = conn.quote_ident(colname)
        t = conn.escape_string(col_type_str(coltype))
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

    # Load CSV into temp table
    def load_temp_data(conn, temp_table_name)

      # Iterate through each row in input CSV file
      rows = []
      # Iterate through each row in input
      reader.each_row do |row_in|
      
        # Perform row-level transform
        row_out = transform_row(row_in)
        values = row_out.fields

        # Now we need to put each value into the SQL string in a format
        # PG will recognize. That means single quotes around strings, etc.
        # To do this we rely on the column types.

        if schema.columns.length != values.length
          raise 'Number of values in row does not match expected schema' 
        end

        # Convert each value to its string representation
        str_values = []
        (0...values.length).to_a.each do |i|
          type = schema.columns.values[i]
          str_values << value_to_db_str(type, conn.escape_string(values[i]))
        end

        # string representation of values for this row
        rows << "(" + str_values.join(", ") + ")"
      end

      sql = <<SQL
insert into #{temp_table_name} values
#{rows.join(",\n")}
;
SQL
      logger.debug(sql)
      conn.exec(sql)
    end

    # Load temp table records into destination table, returning number of
    # affected rows
    def load_destination_table(conn, temp_table_name, dest_table)
      sql = <<SQL
insert into #{conn.quote_ident(dest_table)}
  select * from #{temp_table_name};
SQL
      logger.debug(sql)
      result = conn.exec(sql)

      # return number of rows affected
      result.cmd_tuples
    end

    # Implementation of running the CSV job
    # Reads the input CSV file one row at a time, performs the transform
    # operation, and writes that row to the output.
    def run_internal

      rows_success = rows_error = 0
      msg = ''

      # Perform all steps within a transaction
      @conn.transaction do |conn|

        # Create temp table to match destination table
        temp_table_name = create_temp(conn)

        # Load CSV into temp table
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
