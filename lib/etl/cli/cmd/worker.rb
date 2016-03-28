require_relative '../command'
require 'etl/job/exec'

module ETL::Cli::Cmd
  # Class that handles processing jobs from the queue as a background process
  class Worker < ETL::Cli::Command
    # Starts the infinite loop that processes jobs from the queue
    def execute
      with_log do
        ETL.queue.process_async do |message_info, payload|
          begin
            log.debug("Payload from queue: #{payload.to_s}")
            ETL::Job::Exec.new(payload).run
          rescue StandardError => ex
            # Log and ignore all exceptions. We want other jobs in the queue
            # to still process even though this one is skipped.
            log.exception(ex)
          ensure
            # Acknowledge that this job was handled so we don't keep retrying and 
            # failing, thus blocking the whole queue.
            ETL.queue.ack(message_info)
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
end
