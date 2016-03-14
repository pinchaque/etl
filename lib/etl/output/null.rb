module ETL::Output

  # Dummy ETL class that is used for testing or other behavior simulation
  # Caller can set up number of seconds to sleep or the exception to throw
  # (to simulate error)
  class Null < Base
    # Initialize with the values we will use for the result
    def initialize(params = {})
      super({
        success: 0,
        error: 0,
        sleep: nil,
        exception: nil,
        message: '',
      }.merge(params))
    end

    def run_internal
      sleep(@params[:sleep]) unless @params[:sleep].nil?
      raise ETL::OutputError, @params[:exception] unless @params[:exception].nil?
      ETL::Job::Result.new(@params[:success], @params[:error], @params[:message])
    end
  end
end
