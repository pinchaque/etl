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

    def initialize(reader)
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
      dir = ::File.dirname(output_file)
      FileUtils.mkdir_p(dir) unless Dir.exists?(dir)

      # Temporary location to which we load data
      tmp_id = "etl_#{feed_name}_#{@batch.to_s}"
      tf = Tempfile.new(tmp_id)

      # Open temp output file based on our load strategy
      out_opts = csv_output_options
      case load_strategy
      when :insert_append
        open_opts = "a"
        if ::File.exist?(output_file)
          # Don't write headers
          out_opts[:write_headers] = false 

          # Read the existing output file into temp so that all new lines
          # will get appended to it. This makes a full copy of the data but
          # it ensures that there are no partial failures.
          ::File.open(tf.path, "a") do |outf|
            ::File.open(output_file) do |inf|
              while line = inf.gets
                outf.puts(line)
              end
            end
          end
        end
      when :insert_table
        # Overwrite with headers
        open_opts = "w"
      else
        # Other load strategies not supported by CSV
        raise "Invalid load strategy '#{load_strategy}'"
      end

      # Open output CSV file for writing
      rows_success = rows_error = 0
      logger.debug("Writing to temp CSV output file #{tf.path} with " +
        "file opts #{open_opts}")
      logger.debug(out_opts)
      ::CSV.open(tf.path, open_opts, out_opts) do |csv_out|

        # Iterate through each row in input
        reader.each_row do |row_in|

          # Read our input row into a hash containing all schema columns
          row_out = read_input_row(row_in) 
          
          # Perform row-level transform
          row_out = transform_row(row_out)

          # Write row to output
          csv_out << row_out
          rows_success += 1
        end
      end

      # Move temporary file to final destination
      FileUtils.mv(tf.path, output_file)
      logger.debug("Moving temp CSV file #{tf.path} to final " +
        "destination #{output_file}")

      # Final result
      msg = "Wrote #{rows_success} rows to #{output_file}"
      Result.new(rows_success, rows_error, msg)
    end
  end
end
