require 'etl/queue/payload'
require 'bunny'

module ETL::Queue

  # Class that handles queueing using Bunny gem for RabbitMQ
  class RabbitMQ < Base
    
    def initialize(params)
      @conn = Bunny.new(params[:amqp_uri],
        heartbeat: params[:heartbeat],
        vhost: params[:vhost]
        )
      @conn.start
      @channel = @conn.create_channel(nil, params[:channel_pool_size])
      @channel.prefetch(params[:prefetch_count])
      @queue = @channel.queue(params[:queue], :durable => true)
    end
    
    # Adds the passed in job details to the run queue
    # hash: Contains the following parameters needed to specify which job to run:
    # * source: Source database identifier
    # * dest: Destination database identifier
    # * org: Organization
    # * day: String in YYYY-MM-DD format representing the day
    # * table: Name of the table we're loading
    def enqueue(payload)
      @queue.publish(payload.encode, :persistent => true)
    end

    # Removes all jobs from the queue
    def purge
      @queue.purge
    end
    
    def message_count
      @queue.message_count
    end

    def process_async
      @queue.subscribe(:manual_ack => true, :block => false) do |delivery_info, properties, body|
        payload = ETL::Queue::Payload.decode(body)
        yield delivery_info.delivery_tag, payload
      end
    end
    
    def ack(msg_info)
      @channel.ack(msg_info)
    end
  end
end
