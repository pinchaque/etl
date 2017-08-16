require 'etl/queue/payload'

module ETL::Queue

  # Class that handles queueing using sqs for RabbitMQ
  class Sqs < Base
    def initialize(params = {})
      @region = params.fetch(:region)
      @iam_role = params.fetch(:iam_role)
      @queue_url = params.fetch(:queue_url)

      sts = Aws::STS::Client.new(region: @region)
      session = sts.assume_role(
        role_arn: @iam_role,
        role_session_name: session_name 
      )

      creds = Aws::Credentials.new(
        session.credentials.access_key_id,
        session.credentials.secret_access_key,
        session.credentials.session_token
      )

      client = Aws::SQS::Client.new(
        region: region_name,
        credentials: creds)
      @queue = Aws::SQS::Queue.new({:queue_url: @queue_url, :client: client}) 
      
      # Receive the message in the queue.
      @receive_message_result = @queue.receive_message({
        message_attribute_names: ["All"], # Receive all custom attributes.
        max_number_of_messages: 1, # Receive at most one message.
        wait_time_seconds: 0 # Do not wait to check for the message.
      })
    end
    
    def to_s
      "#{self.class.name}<#{@params[:amqp_uri]}/#{@params[:vhost]}/#{@params[:queue]}>"
    end
    
    def enqueue(payload)
      resp = @queue.send_message({
        message_body: payload.encode,
      })
    end

    # Removes all jobs from the queue
    def purge
      @queue.purge
    end
    
    def message_count
      @queue.get_queue_attributes("ApproximateNumberOfMessages")
    end

    def process_async
      @receive_message_result.messages.each do |message|
        payload = ETL::Queue::Payload.decode(message.body)
        yield message.reciept_handle, payload
      end
    end
    
    def ack(receipt_handle)
        @queue.delete_message({
          receipt_handle: receipt_handle,
        })
    end
  end
end

