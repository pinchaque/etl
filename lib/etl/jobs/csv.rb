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

require 'etl/jobs/file.rb'
require 'tempfile'
require 'fileutils'
require 'csv'

module ETL::Job


  # Writes to CSV files
  class CSV < File

    def initialize
      super
    end

    # File extension of output file
    def output_extension
      "csv"
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns an array of header names to use for the output CSV file.
    def csv_headers
      schema.columns.keys
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

    # Hash of options to give to the output CSV writer. Options should be in
    # the format supported by the Ruby CSV.new() method.
    def csv_output_options
      {
        headers: csv_headers,
        write_headers: true,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }
    end

    # Implementation of running the CSV job
    # Reads the input CSV file one row at a time, performs the transform
    # operation, and writes that row to the output.
    def run_internal

      # Prepare output directory
      outf = output_file
      dir = ::File.dirname(outf)
      FileUtils.mkdir_p(dir) unless Dir.exists?(dir)

      # Temporary location to which we load data
      tmp_id = "etl_#{feed_name}_#{@batch.to_s}"
      tf = Tempfile.new(tmp_id)

      # Open output CSV file for writing
      rows_success = rows_error = 0
      logger.debug("Writing to temp CSV output file #{tf.path}")
      ::CSV.open(tf.path, "w", csv_output_options) do |csv_out|

        # Iterate through each row in input CSV file
        logger.debug("Reading from CSV input file #{input_file}")
        ::CSV.foreach(input_file, csv_input_options) do |row_in|
        
          # Perform row-level transform
          row_out = transform_row(row_in)

          # Write row to output
          csv_out << row_out
          rows_success += 1
        end
      end

      # Move temporary file to final destination
      FileUtils.mv(tf.path, outf)
      logger.debug("Moving temp CSV file to final destination #{outf}")

      # Final result
      msg = "Wrote #{rows_success} rows to #{outf}"
      Result.new(rows_success, rows_error, msg)
    end

  end
end
