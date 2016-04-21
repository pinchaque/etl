module ETL::Input

  # Dummy input class that always returns 0 rows
  class Null < Base

    def each_row(batch = ETL::Batch.new)
      @rows_processed = 0
    end
  end
end
