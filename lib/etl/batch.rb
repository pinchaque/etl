require 'json'

module ETL
  
  # Class that encapsulates the batch parameters for an ETL job
  # To be comparable the Batch uses a hash with sorted keys internally
  # that way we can compare the to_json and to_s representations and they
  # will be equal if-and-only-if the batches are equal.
  class Batch
    def initialize(h = {})
      @hash = h.keys.sort.each_with_object({}) { |k, hash| hash[k] = h[k] }
    end
    
    def to_h
      @hash.dup # duplicate so caller can't change ordering of this hash
    end
    
    def to_json
      @hash.to_json
    end
      
    # Concatenates batch data members separated by underscores. Sorts
    # keys before concatenation so we have deterministic batch ID regardless
    # of order keys were added to hash.
    def to_s
      # get batch values sorted by keys
      v = @hash.values.collect { |x| x.to_s }
      
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
