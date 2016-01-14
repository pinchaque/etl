require 'time'
require 'logger'

module ETL
  
  # Logs to multiple destinations
  class MultiLogger < ::Array
    
    # Delegate each method to contained loggers
    def method_missing(m, *args, &block)
      each do |a|
        a.send(m, *args, &block)
      end
    end
  end
  

  # Logger class that includes time stamp and severity for all messages
  class Logger < ::Logger
    BATCH_PREFIX = "batch_"
    
    attr_accessor :formatter

    def initialize(params = {})
      # file-based log
      if params.has_key?(:file)
        path = params[:file]
        path = "#{ETL.root}/#{path}" unless path.start_with?("/")
        super(path)
      # stdout log
      else
        super(STDERR)
      end
      
      set_severity_string(params.fetch(:level, "info"))
    
      @formatter = Formatter.new
    end
    
    def attributes=(v)
      @formatter.attributes = v
    end
    
    def attributes
      @formatter.attributes
    end
    
    def exception(ex, severity = Logger::ERROR)
      msg = "#{ex.class}: #{ex.message}:\n    "
      if ex.backtrace
        msg += ex.backtrace.join("    \n")
      else
        msg += "<no backtrace available>"
      end
      add(severity) { msg }
    end
    
    # Sets severity level based on string representation of severity (e.g.
    # from config file)
    def set_severity_string(str)
      sev_threshold = Logger.string_to_severity(str)
    end

    # Converts string representation of severity into a Logger constant
    def self.string_to_severity(str)
      case str.downcase
      when "debug"
        Logger::DEBUG
      when "info"
        Logger::INFO
      when "warn"
        Logger::WARN
      when "error"
        Logger::ERROR
      when "fatal"
        Logger::FATAL
      else
        Logger::INFO
      end
    end
    
    # Formatter that includes time stamp and severity. Also provides ability
    # to add job name and batch ID
    class Formatter < ::Logger::Formatter
      attr_accessor :attributes

      def initialize
        @attributes = {}
      end

      # Format the log message
      def call(severity, timestamp, progname, msg)
        str = ""
        if @attributes.has_key?(:job_name)
          str += "{Job=#{@attributes[:job_name]}"
          # Add all our batch attributes
          batch_values = @attributes.select do |k, v| 
            k.to_s.start_with?(BATCH_PREFIX)
          end
          str += ", Batch=#{batch_values}" if batch_values
          str += "} "
        end
        str += String === msg ? msg : msg.inspect
        timestr = timestamp.strftime("%F %T.%L")
        "[#{timestr}] #{severity} #{str}\n"
      end
    end
  end
end
