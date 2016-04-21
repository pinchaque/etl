require 'json'

module ETL
  
  # Class that encapsulates the batch parameters for an ETL job
  # To be comparable the Batch uses a hash with sorted keys internally
  # that way we can compare the to_json and to_s representations and they
  # will be equal if-and-only-if the batches are equal.
  class Batch
    attr_accessor :start_time
    
    def initialize(h = {})
      @hash = h.keys.sort.each_with_object({}) { |k, hash| hash[k] = h[k] }
      @start_time = nil
    end
    
    # Returns hash representation of batch
    def to_h
      @hash.dup # duplicate so caller can't change ordering of this hash
    end
    
    def to_json
      @hash.to_json
    end
    
    # Returns identifier to use for this batch. The identifier is derived from
    # the batch data but is formatted to remove non-word characters. The idea
    # is that this id can uniquely represent this batch for use in naming
    # files, tables, etc. If the batch is empty then this ID is meaningless
    # and this function will return nil.
    def id
      @hash.empty? ? nil : to_s
      v = @hash.values.collect { |x| x.to_s }
      
      # clean up each value
      v.collect! do |x|
        x = "" if x.nil?
        x.downcase.gsub(/[^a-z\d]/, "")
      end
      
      # separate by underscores
      v.empty? ? nil : v.join("_")
    end
      
    def to_s
      kvs = @hash.keys.map { |k| "#{k}=#{@hash[k]}" }.join(",")
      "Batch<#{kvs}>"
    end
  end
end
