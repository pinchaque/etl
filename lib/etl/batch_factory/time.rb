require 'tzinfo'
require 'util/time_util'

module ETL::BatchFactory
  
  # Base class for generating batches that are based on date and time
  # Contains ability to set the time zone for these batches using
  # the TZInfo gem (https://github.com/tzinfo/tzinfo)
  class Time
    
    attr_accessor :tz_str, :lag_secs
    
    def initialize(tz_str = 'UTC', lag_secs = 0)
      @tz_str = tz_str
      @lag_secs = lag_secs
    end
    
    protected
    
    def time_zone
      ::TZInfo::Timezone.get(@tz_str)
    end
    
    def local_time
      time_zone.utc_to_local(::Time.now.utc)
    end
    
    # local time adjusted back by lag_secs so that we can grab data for batches
    # after that time period is ended
    def adj_local_time
      local_time - @lag_secs
    end
  end
  
  # Batches based on year
  class Year < Time
    def generate
      { year: ETL::TimeUtil.round_year(adj_local_time) }
    end
  end
  
  # Batches based on quarter
  class Quarter < Time
    def generate
      { quarter: ETL::TimeUtil.round_quarter(adj_local_time) }
    end
  end
  
  # Batches based on month
  class Month < Time
    def generate
      { month: ETL::TimeUtil.round_month(adj_local_time) }
    end
  end
  
  # Batches based on day
  class Day < Time
    def generate
      { day: ETL::TimeUtil.round_day(adj_local_time) }
    end
  end
  
  # Batches based on hour
  class Hour < Time
    def generate
      { hour: ETL::TimeUtil.round_hour(adj_local_time) }
    end
  end
end
