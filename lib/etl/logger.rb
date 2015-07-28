###############################################################################
# Copyright (C) 2015 Chuck Smith
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
###############################################################################

require 'time'
require 'logger'

module ETL

  # Logger class that includes time stamp and severity for all messages
  class Logger < ::Logger
    attr_accessor :formatter

    def initialize(*args)
      super(*args)
      @formatter = Formatter.new
    end
    
    def exception(ex, severity = Logger::ERROR)
      msg = "#{ex.class}: #{ex.message}:\n    " +
        ex.backtrace.join("    \n")
      add(severity) { msg }
    end
    
    # Formatter that includes time stamp and severity. Also provides ability
    # to add job name and batch ID
    class Formatter < ::Logger::Formatter
      attr_accessor :job_name, :batch

      def initialize
        @job_name = nil
        @batch = nil
      end

      # Format the log message
      def call(severity, timestamp, progname, msg)
        str = ""
        if not @job_name.nil?
          str += "{Job=#{@job_name}"
          str += ", Batch=#{@batch}" unless @batch.nil?
          str += "} "
        end
        str += String === msg ? msg : msg.inspect
        timestr = timestamp.strftime("%F %T.%L")
        "[#{timestr}] #{severity} #{str}\n"
      end
    end
  end
end
