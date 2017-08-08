require 'etl'
require 'etl/mixins/cached_logger'

module ETL::Model
  class JobRun
    include ETL::CachedLogger
    attr_accessor :id, :created_at, :update_at, :job_id, :batch, :status, :queued_at, :started_at, :updated_at, :ended_at, :rows_processed, :message

    def initialize(repository)
      @repository = repository
    end

    # Sets the final status as success along with rows affected
    def success(result)
      final_state(:success.to_s, result)
    end

    # Sets the final status as error along with rows affected
    def error(result)
      final_state(:error.to_s, result)
    end

    # Mark this as an error and save the exception message
    def exception(ex)
      error(ETL::Job::Result.error(ex))
    end

    # Sets the current status as running and initializes started_at
    def running
      self.status = :running.to_s
      self.started_at = Time.now
      self.updated_at = Time.now
      @repository.save(self)
    end

    # Sets the current status as queued and sets queued_at
    def queued()
      self.status = :queued.to_s
      self.queued_at = Time.now
      self.updated_at = Time.now
      @repository.save(self)
    end

    private
    def final_state(state, result)
      self.status = state
      self.updated_at = Time.now
      self.ended_at = Time.now
      self.rows_processed = result.rows_processed
      self.message = result.message
      @repository.save(self)
    end
  end
end
