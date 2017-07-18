require_relative '../command'
require 'etl/job/manager'

module ETL::Cli::Cmd
  # Class that handles putting scheduled jobs into the queue for execution
  class Scheduler < ETL::Cli::Command
    
    option ["-p", "--pause"], "SECS", "seconds to pause between scheduling runs", default: 60 do |s|
      Float(s)
    end
    
    def execute
      ETL.load_user_classes
      with_log do
        while true
          run_iteration
        end
      end
    end
    
    def job_manager
      ETL::Job::Manager.instance
    end
    
    def run_iteration
      log.debug("Start scheduling iteration")
      job_manager.each_class do |klass|
        begin
          process_job_class(klass)
        rescue StandardError => ex
          # Log and ignore all exceptions so that one job doesn't affect others
          log.error("Scheduling job #{j.name} encountered an error")
          log.exception(ex)
        end
      end
      log.debug("End scheduling iteration, sleeping for #{pause} seconds")
      sleep(pause)
    end
    
    # Schedule a job class that has been registered with the manager. To do this
    # we must generate all batches for this job class and instantiate jobs
    # for each to check if they're ready.
    def process_job_class(klass)
      klass.batch_factory.each do |batch|
        process_job(klass.new(batch))
      end
    end
    
    def process_job(job)
      unless job.schedule.ready?
        log.debug("Job #{job} not ready to run, skipping")
        return
      end
      
      payload = ETL::Queue::Payload.new(job.id, job.batch)
      ETL.queue.enqueue(payload)
      log.info("Job #{job} ready to run, enqueued #{payload}")
    end
  end
end
