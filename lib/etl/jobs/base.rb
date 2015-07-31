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
    attr_accessor :feed_name, :schema, :reader, :load_strategy, :batch

    def initialize(reader = nil)
      @reader = reader
      @schema = default_schema
      @load_strategy = :unknown if @load_strategy.nil?
    end
    
    # Returns the default schema for this job. Some derived jobs may be able
    # to determine a default schema based on the destination.
    def default_schema
      nil
    end
    
    # Returns the name of this job. By default we just use the feed name but
    # derived classes may want to override this.
    def name
      @feed_name
    end

    # Returns the ActiveModel Job object
    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = ETL::Model::Job.register(self.class.to_s())
    end

    # Initialize the logger with our job and batch info
    def logger
      l = ETL.logger
      attrs = {
        job_name: name,
        feed_name: @feed_name,
        load_strategy: @load_strategy,
        job_class_name: model().class_name,
        input_rows_processed: @reader.nil? ? nil : @reader.rows_processed,
        input_name: @reader.nil? ? "N/A" : @reader.name,
      }
      # Add the batch prefix so we can find the batch id later
      @batch.each do |k, v|
        key = ETL::Logger::BATCH_PREFIX + k.to_s
        attrs[key.to_sym] = v
      end
      l.attributes = attrs
      l
    end

    # Returns string representation of batch suitable as an identifier
    # Concatenates batch data members separated by underscores. Sorts
    # keys before concatenation so we have deterministic batch ID regardless
    # of order keys were added to hash.
    def batch_id
      return nil if @batch.nil?
      
      # get batch values sorted by keys
      values = @batch.sort.collect { |x| x[1] }
      
      # clean up each value
      values.collect! do |x|
        x = "" if x.nil?
        x.downcase.gsub(/[^a-z\d]/, "")
      end
      
      # separate by underscores
      values.join("_")
    end

    # Runs the job for the batch, keeping the status updated and handling
    # exceptions.
    def run(b)
      @batch = b
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
