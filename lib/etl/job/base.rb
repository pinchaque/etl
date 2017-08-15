require 'mixins/cached_logger'
require 'slack/notifier'

module ETL::Job

  # Base class for all jobs that are run
  class Base
    include ETL::CachedLogger
    
    attr_reader :batch, :notifier
    
    def initialize(b)
      @batch = b
      @notifier ||= begin 
        if ETL.config.core[:slack]
          slack_config = ETL.config.core[:slack]
          if slack_config[:url] && slack_config[:channel] && id 
            ETL::Slack::Notifier.new(slack_config[:url], slack_config[:channel], id)
          end
        end
      end
    end
    
    # Registers a job class with the manager. This is typically called by
    # subclasses to register themselves with a convenient id to represent
    # that subclass. Only registered jobs can be executed.
    def self.register_job
      Manager.instance.register(id, self)
    end
    
    # Registers a job class that depends on parent job with the manager
    def self.register_job_with_parent(p_id)
      Manager.instance.register_job_with_parent(id, p_id, self)
    end

    # Run the job by instantiating input and output classes with parameters
    # and then running the output class for this batch
    def run
      # set up our input object
      inp = input
      inp.log = log
      log.debug("Input: #{inp.name}")
      inp.slack_tags.map { |atr, value| @notifier.add_text_to_attachments("# #{atr.to_s}: #{value}") } if @notifier
      
      # set up our output object
      out = output
      out.log = log
      out.reader = inp
      log.debug("Output: #{out.feed_name}")
      
      # run this batch
      log.debug("Batch: #{@batch.to_s}")
      out.batch = @batch
      out.run
    end
    
    # Default class function for getting ID based on class name
    def self.id
      ETL::StringUtil::camel_to_snake(ETL::StringUtil::base_class_name(name.to_s))
    end

    # Default function for getting ID based on class name
    def id
      self.class.id
    end
    
    def to_s
      batch_id = (@batch && @batch.id) || "NO_BATCH"
      "#{id}<#{batch_id}>"
    end
    
    # Instantiates schedule for this job
    def schedule
      @schedule ||= self.class.schedule_class.new(self, @batch)
    end
  
    def self.schedule_class
      ETL::Schedule::Never
    end
    
    def self.batch_factory
      batch_factory_class.new
    end
    
    def self.batch_factory_class
      ETL::BatchFactory::Base
    end
  
    # Instantiates the input class for this job. Derived job classes should
    # override this method to create the correct input object for the job.
    def input
      ETL::Input::Null.new
    end
    
    # Instantiates the output class for this job. Derived job classes should
    # override this method to create the correct output object for the job.
    def output
      ETL::Output::Null.new
    end

    def metrics
      @metrics ||= ETL.create_metrics
    end

    protected
    def log_context
      {
          job: id.to_s,
          batch: (@batch && @batch.id) || "nil",
      }
    end
  end
end
