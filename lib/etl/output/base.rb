require 'etl/mixins/cached_logger'

module ETL::Output

  # Base class for all ETL jobs
  class Base
    include ETL::CachedLogger
    attr_accessor :reader, :batch
    
    def initialize
      @reader ||= ETL::Input::Base.new
      @schema = nil # lazy load using default_schema
    end
    
    # Returns the default schema for this job. Some derived jobs may be able
    # to determine a default schema based on the destination.
    def default_schema
      nil
    end
    
    # Name of this data feed. We use the class name as default with the 
    # assumption that each derived class is a different feed. Note that feed_name
    # cannot be nil because we use it as an identifier in many places. This could
    # happen if the derived class is anonymous, in which case the user must
    # override this method with a non-nil name.
    def feed_name
      raise "Invalid nil feed_name() in Output class #{self.class}" unless self.class.name
      ETL::StringUtil.camel_to_snake(self.class.name).gsub(/::/, '_')
    end
    
    def log_context
      { 
        class: self.class.name.to_s,
        batch: batch_id,
      }
    end

    # Returns string representation of batch suitable as an identifier
    # Returns nil if there is no batch or it's emtpy, in which case we don't
    # have a suitable identifier
    def batch_id
      @batch && @batch.id
    end

    # Runs the job for the batch, keeping the status updated and handling
    # exceptions.
    def run
      log.info("Running...")
      result = run_internal()
      log.info("Success! #{result.message}")
      return result
    end
    
    # Returns the schema for this job, preferring the default schema (which
    # may be provided by subclasses) or just an empty one.
    def schema
      @schema ||= (default_schema || ETL::Schema::Table.new)
    end

    # Helper function for output schema definition DSL
    def define_schema
      yield schema if block_given?
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
