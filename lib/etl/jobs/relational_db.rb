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


  # Writes to Relational Databases
  class RelationalDB < Base

    # Initialize given a connection to the database
    def initialize(conn)
      super()
      @conn = conn
    end

    # File extension of output file
    def output_extension
      "csv"
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Hash of options to give to the input CSV reader. Options should be in
    # the format supported by the Ruby CSV.new() method.
    def csv_input_options
      {
        headers: true,
        return_headers: false,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }
    end

    # Creates a temporary CSV file that we can later use to load into 
    # the DB. Returns path to the file.
   def create_tmp_csv(batch)
      # Temporary location to which we load data
      tmp_id = "etl_#{feed_name}_#{batch.to_s}"
      tf = Tempfile.new(tmp_id)

      # Output options need to match how the DB will load this file
      csv_output_options = {
        write_headers: false,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }

      # Open output CSV file for writing
      logger(batch).debug("Writing to temp CSV output file #{tf.path}")
      ::CSV.open(tf.path, "w", csv_output_options) do |csv_out|

        # Iterate through each row in input CSV file
        logger(batch).debug("Reading from CSV input file #{input_file}")
        ::CSV.foreach(input_file, csv_input_options) do |row_in|
        
          # Perform row-level transform
          row_out = transform_row(row_in)

          # Write row to output
          csv_out << row_out
        end
      end

      tf.path
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

    # Creates a temp table for the specified batch in the specified 
    # connection transaction. Returns the name of this temp table.
    def create_temp(conn, batch)
      # Get string representation of all our columns
      type_ary = []
      schema.columns.each do |colname, coltype|
        n = conn.escape_string(colname)
        t = conn.escape_string(col_type_str(coltype))
        type_ary << "#{n} #{t}"
      end

      temp_table_name = "#{feed_name}_#{batch.to_s}_#{SecureRandom.hex(8)}"
      temp_table_name.gsub!(/\W/, '')
      temp_table_name = conn.escape_string(temp_table_name)
      sql = "create temp table #{temp_table_name} (#{type_ary.join(', ')});"
      logger(batch).debug(sql)
      conn.exec(sql)
      return temp_table_name
    end

    # Load CSV into temp table
    def load_temp_data(conn, batch, filename, temp_table_name)
      sql = <<SQL
copy #{temp_table_name} 
from '#{conn.escape_string(filename)}' 
with (format csv);
SQL
      logger(batch).debug(sql)
      conn.exec(sql)
    end

    # Load temp table records into destination table
    def load_destination_table(conn, batch, temp_table_name, dest_table)
      sql = <<SQL
insert into #{conn.escape_string(dest_table)}
  select * from #{temp_table_name};
SQL
      logger(batch).debug(sql)
      conn.exec(sql)
    end

    # Implementation of running the CSV job
    # Reads the input CSV file one row at a time, performs the transform
    # operation, and writes that row to the output.
    def run_internal(batch)

      rows_success = rows_error = 0

      # Copy input into temp CSV file
      filename = create_tmp_csv(batch)

      # Perform all steps within a transaction
      @conn.transaction do |conn|

        # Create temp table to match destination table
        temp_table_name = create_temp(conn, batch)

        # Load CSV into temp table
        load_temp_data(conn, batch, filename, temp_table_name)

        # Load temp table records into destination table
        dest_table = feed_name
        rows_success = load_destination_table(conn, batch, temp_table_name, dest_table)
        msg = "Wrote #{rows_success} rows to #{dest_table}"
      end

      # Final result
      Result.new(rows_success, rows_error, msg)
    end

  end
end
