module ETL::BatchFactory
  class Base

    # Generates an empty Batch class
    def generate
      return ETL::Batch.new
    end
  end
end
