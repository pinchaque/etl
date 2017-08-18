require 'mixins/cached_logger'

module ETL::JobFactory

  # Base class for all job factories that are run
  # enables one job to represent multiple jobs in a
  # data driven way.
  class Base
    include ETL::CachedLogger

    # create can create a specified job
    def create(job_id, batch)
      klass = ::ETL::Job::Manager.instance.get_class(job_id)
      klass.new(batch)
    end
  end
end
