require 'mixins/cached_logger'

module ETL::Input
  class Base
    include ETL::CachedLogger
    attr_accessor :rows_processed, :row_transform, :col_transforms

    def initialize(params = {})
      @rows_processed = 0
      @row_transform = nil
      @col_transforms = {}
      @params = params
    end
    
    # Name of this input that is used in logging to help describe where the
    # data is coming from.
    def name
      self.class.name
    end
    
    # Reads each row from the input file and passes it to the specified
    # block. By default does nothing, which is likely an error.
    def each_row
      log.warning("Called ETL::Input::Base::each_row()")
    end

    # Reads rows in slices of specified size from the input source and
    # passes them as an array to the specified block. By default we just
    # put a wrapper around each_row that collects rows into an array. 
    # Derived classes can implement more intelligent slicing logic if the 
    # input source supports it.
    def each_row_slice(slice_size = 100)
      slice = []
      each_row do |row_in|
        slice << row_in
        if slice.length >= slice_size
          yield slice
          slice = []
        end
      end
      yield slice if slice.length > 0
    end

    # Runs all our defined transforms on rows
    def transform_row!(row)
      @row_transform.call(row) unless @row_transform.nil?
      @col_transforms.each do |name, func|
        next unless row.has_key?(name)
        if func.respond_to?(:transform)
          row[name] = func.transform(row[name])
        else
          row[name] = func.call(row[name])
        end
      end
    end
    
    def log_context
      { name: name, }
    end
  end
end
