
module ETL::Job

  class Batch
  end

  class DateBatch < Batch
    attr_accessor :date

    def initialize(year = nil, month = nil, day = nil)
      if year.nil?
        @date = Date.new
      elsif year.class.name == "Date"
        @date = year.clone
      else
        month = 1 if month.nil?
        day = 1 if day.nil?
        @date = Date.new(year, month, day)
      end
    end

    # Convert to string as ISO8601 YYYY-MM-DD
    def to_s
      @date.strftime('%F')
    end

    def to_str
      to_s
    end
  end
end
