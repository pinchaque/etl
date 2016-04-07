module ETL::Job

  # Class that represents the result of an ETL job run
  class Result
    attr_accessor :status, :rows_processed, :message
      
    def self.success(rows = 0, msg = '')
      r = self.new(:success)
      r.rows_processed = rows
      r.message = msg
      r
    end
    
    def self.error(ex = nil)
      r = self.new(:error)
      
      if ex.is_a?(Exception)
        r.message = ex.message + "\n" + ex.backtrace.join("\n")
      else
        r.message = ex.to_s
      end
      
      r
    end

    def initialize(status)
      @status = status
    end
    
    def success?
      @status == :success
    end
  
    def to_s
      "Result<status=#{@status} rows=#{@rows_processed} msg='#{@message}'>"
    end
  end
end
