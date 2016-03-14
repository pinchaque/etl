module ETL
  
  # Class that encapsulates the batch parameters for an ETL job
  class Batch
    attr_reader :hash
    
    def initialize(h = {})
      @hash = h.to_h
    end
    
    def to_h
      @hash
    end
      
    # Concatenates batch data members separated by underscores. Sorts
    # keys before concatenation so we have deterministic batch ID regardless
    # of order keys were added to hash.
    def to_s
      # get batch values sorted by keys
      v = @hash.sort.collect { |x| x[1] }
      
      # clean up each value
      v.collect! do |x|
        x = "" if x.nil?
        x.downcase.gsub(/[^a-z\d]/, "")
      end
      
      # separate by underscores
      v.join("_")
    end
  end
end
