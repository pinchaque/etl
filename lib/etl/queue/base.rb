require 'etl/queue/payload.rb'

module ETL::Queue

  # Base class that defines the interface our work queues need
  class Base
    # Starts async processing of the queue. When an element is read off the 
    # queue the |message_info, payload| is passed to block.
    def process_async(&block)
    end
    
    # Places the specified payload onto the queue for processing by a worker.
    def enqueue(payload)
    end
    
    # Purges all jobs from the queue
    def purge
    end

    # Returns number of messages in the queue
    def message_count
      0
    end
    
    # Acknowledges that the specified message d
    def ack(msg_info)
    end
  end
end
