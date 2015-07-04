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

module ETL::Job

  # Base class for all ETL jobs
  class Base
    attr_accessor :feed_name, :schema, :reader, :load_strategy

    def initialize(reader)
      @reader = reader
      @schema = nil
      @load_strategy = :unknown
    end

    # Returns the ActiveModel Job object
    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = Job.register(self.class.to_s())
    end

    # Initialize the logger with our job and batch info
    def logger
      l = Rails.logger
      l.formatter.job_name = model().class_name
      l.formatter.batch = @batch
      l
    end

    # Runs the job for the batch, keeping the status updated and handling
    # exceptions.
    def run(batch)
      @batch = batch
      jr = model().create_run(@batch)

      begin
        logger.info("Running...")
        jr.running()
        result = run_internal()
        logger.info("Success! #{result.num_rows_success} rows; "\
          + "#{result.num_rows_error} errors; #{result.message}")
        jr.success(result)
      rescue Exception => ex
        logger.error("Error: #{ex}")
        ex.backtrace.each do |x|
          logger.error("    #{x}")
        end
        result = Result.new
        result.message = ex.message
        jr.error(result)
      end

      return jr
    end

    # Helper function for output schema definition DSL
    def define_schema
      @schema = ETL::Schema::Table.new if @schema.nil?
      yield @schema if block_given?
    end

    # Processes a row read from the input stream and returns a row that
    # has all the columns from schema
    def read_input_row(row)
      row_out = {}
      schema.columns.each do |name, col|
        # use nil if row doesn't exist in input
        row_out[name] = row.fetch(name, nil)
      end
      row_out 
    end
  end
end
