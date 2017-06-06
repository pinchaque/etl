module ETL::Input
 
  class Day < Base
    attr_accessor :full_date, :day_of_week_number, :day_of_week_name, :day_of_month, :day_of_year, :weekday_flag, :weekend_flag, :week_number, :month_number, :month_name, :quarter, :quarter_month,:year, :year_month, :year_quarter

    def initialize(d) 
      quarter_num = ((d.mon) / 3.0 ).ceil
      quarter_month = (d.mon + 2) % 3  + 1
      @full_date = d.strftime('%Y/%m/%d')
      @day_of_week_number = d.wday
      @day_of_week_name = d.strftime('%A')
      @day_of_month = d.mday
      @day_of_year = d.yday
      @weekday_flag = ((d.saturday? || d.sunday?) ? false : true)
      @weekend_flag = ((d.saturday? || d.sunday?) ? true : false)
      @week_number = d.cweek
      @month_number = d.mon
      @month_name = d.strftime('%B')
      @quarter = quarter_num
      @quarter_month = quarter_month
      @year = d.year
      @year_month = d.strftime('%Y/%m')
      @year_quarter = d.strftime("%Y/Q#{quarter_num}")
    end
  end

  class DateTable < Base
    attr_accessor :start_date, :end_date

    def initialize(start_date, end_date)
      @start_date = start_date
      @end_date = end_date
    end

    # Builds the date rows based on the start and end date provided.
    def each_row(batch = ETL::Batch.new)
      log.debug("Building date table starting from date #{start_date} to #{end_date}\n")
      current_date = start_date
      # We are expecting a result like:
      days = []
      while current_date <= end_date
        days << Day.new(current_date)
        current_date = current_date.next_day(1)
      end
      return days
    end

    # Builds the date rows based on the start and end date provided.
    def build_day(d)
      # We are expecting a result like:
      @day = Day.new(d)
    end
  end
end
