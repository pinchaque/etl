module ETL::Input

  class Array < Base
    
    attr_accessor :data

    # Construct reader with array of hashes that we feed back
    def initialize(d = [])
      super()
      @data = d
    end

    # Regurgitates data from array passed on construction
    def each_row(batch = ETL::Batch.new)
      @rows_processed = 0
      @data.each do |h|
        h = h.clone
        transform_row!(h)
        yield h
        @rows_processed += 1
      end
    end
  end
end
