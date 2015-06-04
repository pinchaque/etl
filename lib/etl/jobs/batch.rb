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
