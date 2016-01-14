module ETL::Input

  class Array < Base

    # Construct reader with array of hashes that we feed back
    def initialize(params = {})
      super(params)
    end
    
    def data
      @params[:data]
    end

    # Regurgitates data from array passed on construction
    def each_row
      @rows_processed = 0
      data.each do |h|
        h = h.clone
        transform_row!(h)
        yield h
        @rows_processed += 1
      end
    end
  end
end
