require_relative '../command'

module ETL::Cli::Cmd
  class Queue < ETL::Cli::Command
    class Print < ETL::Cli::Command
      def execute
        log.info("Job queue: #{ETL.queue.to_s}")
        log.info("Jobs in queue: #{ETL.queue.message_count}")
      end
    end
    
    class Purge < ETL::Cli::Command
      option ['-y', '--yes'], :flag, 'Really proceed with purge'
      
      def execute
        num_msg = ETL.queue.message_count
        if yes?
          log.info("Purging queue of #{num_msg} jobs")
          ETL.queue.purge
        else
          log.info("There are #{num_msg} jobs in queue; re-run with -y to proceed.")
        end
      end
    end
    
    subcommand 'print', 'Prints current queue details and status', Queue::Print
    subcommand 'purge', 'Purges all jobs from queue', Queue::Purge
  end
end
