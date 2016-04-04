require 'tzinfo'
require 'util/time_util'

module ETL::BatchFactory
  
  # Base class for generating batches that are based on date and time
  # Contains ability to set the time zone for these batches using
  # the TZInfo gem (https://github.com/tzinfo/tzinfo)
  class Time < Base
    
    attr_accessor :tz_str, :lag_secs
    
    def initialize(tz_str = 'UTC', lag_secs = 0)
      @tz_str = tz_str
      @lag_secs = lag_secs
    end
    
    def generate
      # generate from how the derived class creates the hash from the adjusted
      # local time
      ETL::Batch.new(hash_from_time(adj_local_time))
    end
    
    # Converts times in our hash to Time objects
    def from_hash(h)
      h = h.dup
      if h.has_key?(:time)
        begin
          h[:time] = DateTime.parse(h[:time]).to_time.utc
        rescue StandardError => ex
          # ignore
        end
      end
      super(h)
    end
    
    def validate!(batch)
      h = batch.to_h
      
      if h.size != 1
        raise ETL::BatchError, "Invalid batch #{h} specified; expected only one key-value"
      end
      
      t = h.values[0]
      unless t.is_a?(::Time)
        raise ETL::BatchError, "Invalid batch #{h} specified; expected value to be a Time class (was #{t.class.name})"
      end
      
      # generate the batch has from our derived class, which will do rounding
      exp = hash_from_time(t)
      
      # keys should match
      if exp.keys != h.keys
        raise ETL::BatchError, "Invalid batch #{h} specified; expected keys to be #{exp.keys}"
      end
      
      # values should match, otherwise rounding problem
      if exp.values != h.values
        raise ETL::BatchError, "Invalid batch #{h} specified; value is not rounded properly"
      end
      
      batch
    end
    
    protected
    
    def hash_from_time(t)
      { time: round(t) }
    end
    
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
    
    # Rounds time to the appropriate granularity. Override in derived classes.
    def round(t)
      t
    end
  end
  
  # Batches based on year
  class Year < Time
    def round(t)
      ETL::TimeUtil.round_year(t)
    end
  end
  
  # Batches based on quarter
  class Quarter < Time
    def round(t)
      ETL::TimeUtil.round_quarter(t)
    end
  end
  
  # Batches based on month
  class Month < Time
    def round(t)
      ETL::TimeUtil.round_month(t)
    end
  end
  
  # Batches based on day
  class Day < Time
    def round(t)
      ETL::TimeUtil.round_day(t)
    end
  end
  
  # Batches based on hour
  class Hour < Time
    def round(t)
      ETL::TimeUtil.round_hour(t)
    end
  end
end
