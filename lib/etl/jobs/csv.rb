require 'etl/jobs/file.rb'
require 'tempfile'
require 'fileutils'
require 'csv'

module ETL::Job

  class CSV < File

    def initialize
      super
      @header = true
      @column_separator = ","
      @row_separator = "\n"
      @quote_char = '"'
    end

    def output_extension
      "csv"
    end

    def transform_row(row)
      row
    end

    def csv_headers
      schema.columns.keys
    end

    def csv_input_options
      {
        headers: @header,
        return_headers: false,
        col_sep: @column_separator,
        row_sep: @row_separator,
        quote_char: @quote_char
      }
    end

    def csv_output_options
      {
        headers: csv_headers,
        write_headers: true,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }
    end

    def run_internal(batch)

      # Prepare output directory
      outf = output_file(batch)
      dir = ::File.dirname(outf)
      FileUtils.mkdir_p(dir) unless Dir.exists?(dir)

      # Temporary location to which we load data
      tmp_id = "etl_#{feed_name}_#{batch.to_s}"
      tf = Tempfile.new(tmp_id)

      # Open output CSV file for writing
      rows_success = rows_error = 0
      ::CSV.open(tf.path, "w", csv_output_options) do |csv_out|

        # Iterate through each row in input CSV file
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

      # Final result
      msg = "Wrote #{rows_success} rows to #{outf}"
      Result.new(rows_success, rows_error, msg)
    end

  end
end
