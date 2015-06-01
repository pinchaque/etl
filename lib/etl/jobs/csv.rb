require 'etl/jobs/file.rb'
require 'tempfile'
require 'fileutils'
require 'csv'

module ETL::Job

  class CSV < File
    attr_accessor :header_lines

    def output_extension
      "csv"
    end

    def transform_row(row)
      row
    end

    def run_internal(batch)

      # Temporary location to which we load data
      tmp_id = "etl_#{feed_name}_#{batch.to_s}"
      tf = Tempfile.new(tmp_id)
      puts("*** using tmp path #{tf.path}")

      # Open output CSV file for writing
      ::CSV.open(tf.path, "w") do |csv_out|

        # Iterate through each row in input CSV file
        ::CSV.foreach(input_file) do |row_in|

          # Perform row-level transform
          row_out = transform_row(row_in)

          # Write row to output
          csv_out << row_out
        end
      end

      # Move temporary file to final destination
      # XXX need to create output directory
      FileUtils.mv(tf.path, output_file(batch))
    end

  end
end
