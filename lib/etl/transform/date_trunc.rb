require 'etl/transform/base.rb'

module ETL::Transform

  # Truncates date/time strings to the specified resolution
  class DateTrunc < Base

    # Construct transform with degree of resolution
    def initialize(resolution)
      super()
      @resolution = resolution
    end

    # Truncates the date
    def transform(str)
      begin
        dt = DateTime.parse(str.to_s) 
      rescue ::ArgumentError => ex
        # Ignore ill-formatted strings
        return nil
      end

      # week needs special handling because it can wrap month/year boundaries
      if @resolution == :week
        dt -= dt.wday
        dt = DateTime.new(dt.year, dt.mon, dt.mday)
      else
        y, m, d, h, mi = dt.year, dt.month, dt.mday, dt.hour, dt.minute
        case @resolution
        when :minute
          # already rounded to minute

        when :hour
          mi = 0

        when :day
          h = mi = 0

        when :month 
          h = mi = 0
          d = 1

        when :quarter
          h = mi = 0
          d = 1
          m -= ((m - 1) % 3) # start of qtr

        when :year
          h = mi = 0
          m = d = 1

        else
          raise ::ArgumentError, "Invalid resolution '#{@resolution.to_s}'"
        end

        # Create truncated object and return string time representation
        dt = DateTime.new(y, m, d, h, mi, 0)
      end

      dt.strftime("%F %T")
    end
  end
end
