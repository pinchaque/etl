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

module ETL::Job

  # Dummy ETL class that is used for testing or other behavior simulation
  # Caller can set up number of seconds to sleep or the exception to throw
  # (to simulate error)
  class Dummy < Base
    attr_accessor :exception, :sleep_time

    # Initialize with the values we will use for the result
    def initialize(rows_success = 0, rows_error = 0, msg = '')
      @rows_success = rows_success
      @rows_error = rows_error
      @msg = msg
    end

    def run_internal
      sleep(@sleep_time) unless @sleep_time.nil?
      raise @exception unless @exception.nil?
      Result.new(@rows_success, @rows_error, @msg)
    end
  end
end
