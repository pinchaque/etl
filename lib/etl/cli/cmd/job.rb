require_relative '../command'

module ETL::Cli::Cmd
  class Job < ETL::Cli::Command
    
    class List < ETL::Cli::Command
      def execute
        ETL.load_user_classes
        log.info("List of registered job IDs (classes):")
        ETL::Job::Manager.instance.job_classes.each do |id, klass|
          log.info("  * #{id} (#{klass.name.to_s})")
        end
      end
    end
    
    class Run < ETL::Cli::Command
      parameter "JOB_ID", "ID of job we are running", required: true
      
      option ['-b', '--batch'], "BATCH", "Batch for the job in JSON or 'key1=value1;key2=value2' format", attribute_name: :batch_str
      option ['-q', '--queue'], :flag, "Queue the job instead of running now"
      
      def execute
        ETL.load_user_classes
        
        if @batch_str # user-specified batch
          begin
            batch = batch_factory.parse!(@batch_str)
          rescue StandardError => ex
            raise ArgumentError, "Invalid batch value specified (#{ex.message})" 
          end
          run_batch(batch)
        else # need to generate the batch(es) from the job
          batch_factory.each do |b|
            run_batch(b)
          end
        end
      end
      
      def job_class
        @job_class ||= ETL::Job::Manager.instance.get_class(job_id)
        raise "Failed to find specified job ID '#{job_id}'" unless @job_class
        @job_class
      end
      
      def batch_factory
        @batch_factory ||= job_class.batch_factory_class.new
      end
      
      # runs the specified batch
      def run_batch(b)
        run_payload(ETL::Queue::Payload.new(job_id, b))
      end
      
      # enqueues or runs specified payload based on param setting
      def run_payload(payload)
        if queue?
          log.info("Enqueuing #{payload}")
          ETL.queue.enqueue(payload)
        else
          log.info("Running #{payload}")
          ETL::Job::Exec.new(payload).run
        end
      end
    end
    
    subcommand 'list', 'Lists all jobs registered with ETL system', Job::List
    subcommand 'run', 'Runs (or enqueues) specified jobs + batches', Job::Run
  end
end
