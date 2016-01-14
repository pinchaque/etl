module ETL::Process

  # Class that handles processing jobs from the queue as a background process
  class Worker < Base
    # Starts the infinite loop that processes jobs from the queue
    def run_process
      queue.process_async do |message_info, payload|
        begin
          log.debug("Payload from queue: #{payload.to_s}")
          ETL::Job.new(payload).run
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
        ensure
          # Acknowledge that this job was handled so we don't keep retrying and 
          # failing, thus blocking the whole queue.
          queue.ack(message_info)
        end
      end

      # Just sleep indefinitely so the program doesn't end. This doesn't pause the
      # above block.
      while true
        sleep(10)
      end
    end
  end
end
