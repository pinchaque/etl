module ETL::Input

  class FiscalQuarter
    attr_accessor :quarter_lookup, :quarter_month_num_lookup, :fiscal_start_month

    def initialize(fiscal_start_month)
      if fiscal_start_month < 1 || fiscal_start_month > 12
        raise ArgumentError "Argument is not a valid month between 1 to 12"
      end

      @quarter_lookup = {}
      @quarter_month_num_lookup = {}
      @fiscal_start_month = fiscal_start_month
      curr_month = fiscal_start_month
      curr_quarter = 1
      curr_quarter_mon_num = 1

      for i in 1..12
        @quarter_lookup[curr_month] = curr_quarter
        @quarter_month_num_lookup[curr_month] = curr_quarter_mon_num

        curr_month +=1
        if (i % 3) == 0
          curr_quarter +=1
          curr_quarter_mon_num = 1
        else
          curr_quarter_mon_num += 1
        end

        if curr_month > 12
          curr_month = 1
        end
      end
    end

    def calculate_fiscal_year(d)
      calc_year = d.year + 1
      if d.mon < @fiscal_start_month or @fiscal_start_month == 1
          calc_year = d.year
      end
      calc_year
    end
  end

  class Day
    ATTRS = [:id, :full_date, :day_of_week_number, :day_of_week_name, :day_of_month, :day_of_year, :weekday_flag, :weekend_flag, :week_number, :month_number, :month_name, :quarter, :quarter_month,:year, :year_month, :year_month_int, :year_quarter, :fiscal_year, :fiscal_quarter, :fiscal_quarter_month]
    attr_accessor *ATTRS

    def initialize(d, fiscal_map)
      quarter_num = ((d.mon) / 3.0 ).ceil
      quarter_month = (d.mon + 2) % 3  + 1
      fiscal_quarter_num = fiscal_map.quarter_lookup.fetch(d.mon)
      fiscal_quarter_mon_num = fiscal_map.quarter_month_num_lookup.fetch(d.mon)

      @id = d.strftime('%Y%m%d').to_i
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
      @year_month_int = d.strftime('%Y%m').to_i
      @year_quarter = d.strftime("%Y/Q#{quarter_num}")
      @fiscal_year = fiscal_map.calculate_fiscal_year(d)
      @fiscal_quarter = "#{@fiscal_year}/Q#{fiscal_quarter_num}"
      @fiscal_quarter_month = fiscal_quarter_mon_num
    end

    def values
      ATTRS.map do |a|
        [a.to_s, send(a)]
      end
    end

  end

  class DateTable < Base
    attr_accessor :fiscal_start_month, :start_date, :end_date

    def initialize(fiscal_start_month, start_date, end_date)
      @fiscal_start_month = fiscal_start_month
      @start_date = start_date
      @end_date = end_date
    end

    # Builds the date rows based on the start and end date provided.
    def each_row(batch = ETL::Batch.new)
      fiscal_map = FiscalQuarter.new(@fiscal_start_month)
      log.debug("Building date table starting from date #{start_date} to #{end_date}\n")
      for d in start_date..end_date
        day = Day.new(d, fiscal_map)
        yield day
      end
    end
  end
end
