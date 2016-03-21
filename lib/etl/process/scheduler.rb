require 'etl/process/base'
require 'etl/job/job_manager'

module ETL::Process

  # Class that handles putting scheduled jobs into the queue for execution
  class Scheduler < Base
    
    def option_parser
      super do |opts|
        opts.on("-j", "--job-file FILE", "File listing all of the jobs") do |file|
          @options[:jobs_file] = file
        end
        opts.on("-p", "--pause SECS", "Seconds to pause between scheduling runs") do |secs|
          @options[:pause] = secs
        end
      end
    end
    
    def pause_secs
      @options[:pause] || 60
    end
    
    def required_args
      super + [:jobs_file]
    end
    
    # Starts the infinite loop that schedules jobs
    def run_process
      while true
        run_iteration
        log.debug("Sleeping for #{pause_secs} seconds")
        sleep(pause_secs)
      end
    end
    
    def run_iteration
      log.debug("Start scheduling iteration")
      jobs = ETL::Config.load_file(@options[:jobs_file])
      jm = ETL::Job::Manager.new(jobs)
      jm.each_class do |job_class|
        begin
          process_job_class(job_class)
        rescue StandardError => ex
          # Log and ignore all exceptions so that one job doesn't affect others
          log.error("Scheduling job #{j.name} encountered an error")
          log.exception(ex)
        end
      end
      log.debug("End scheduling iteration")
    end
    
    # Schedule a job class that has been registered with the manager. To do this
    # we must generate all batches for this job class and instantiate jobs
    # for each to check if they're ready.
    def process_job_class(job_class)
      batch_fact = job_class.batch_factory_class.new
      batch_fact.each do |batch|
        process_job(job_class.new(batch))
      end
    end
    
    def process_job(job)
      unless job.schedule.ready?
        log.debug("Job #{job} not ready to run, skipping")
        return
      end
      
      payload = ETL::Queue::Payload.new(job.id, job.batch)
      ETL.queue.enqueue(payload)
      log.debug("Job #{job} ready to run, enqueued #{payload}")
    end
  end
end
