require 'etl/core'
require 'etl/queue/payload'

module ETL::Queue

  # Class that handles queueing using sqs for RabbitMQ
  class SQS < Base
    def initialize(params = {})
      if params.empty?
        params = ::ETL.config.sqs
      end

      idle_timeout = params.fetch(:idle_timeout, nil)
      @queue_url = params.fetch(:url, '')
      @region = params.fetch(:region, '')
      @iam_role = params.fetch(:iam_role, '')

      creds = ::ETL.create_aws_credentials(@region, @iam_role, "etl_sqs_session")

      @client = Aws::SQS::Client.new(region: @region, credentials: creds)
      @poller = Aws::SQS::QueuePoller.new(@queue_url, { client: @client, idle_timeout: idle_timeout })
      @queue = Aws::SQS::Queue.new(url: @queue_url, client: @client)

      # Receive the message in the queue.
      @receive_message_result = @queue.receive_messages({
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
      resp = @client.get_queue_attributes(queue_url: @queue_url, attribute_names: ["ApproximateNumberOfMessages"])
      resp.attributes["ApproximateNumberOfMessages"].to_i
    end

    def process_async
      @poller.poll(skip_delete: true) do |message|
        payload = ETL::Queue::Payload.decode(message.body)
        yield message, payload
      end
    end

    def ack(message)
      @queue.delete_messages({
        entries: [
          {
            id: message.message_id,
            receipt_handle: message.receipt_handle,
          },
        ],
      })
    end
  end
end

