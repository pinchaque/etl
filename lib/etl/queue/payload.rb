require 'json'
require 'etl/util/hash_util'
module ETL::Queue

  # Class representing the payload we are putting into and off the job queue
  class Payload
    attr_reader :job_id, :batch
    
    def initialize(job_id, batch)
      @job_id = job_id
      @batch = batch
    end

    def to_s
      "Payload<job_id=#{@job_id}, batch=#{@batch.to_s}>"
    end
    
    # encodes the payload into a string for storage in a queue
    def encode
      h = {
        :batch => @batch.to_h,
        :job_id => @job_id
      }
      h.to_json.to_s
    end
    
    # creates a new Payload object from a string that's been encoded (e.g.
    # one that's been popped off a queue)
    def self.decode(str)
      h = ETL::HashUtil.symbolize_keys(JSON.parse(str))
      ETL::Queue::Payload.new(h[:job_id], ETL::Batch.new(h[:batch]))
    end
  end
end
