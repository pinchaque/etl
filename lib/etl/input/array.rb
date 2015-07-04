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

require 'etl/input/base.rb'

module ETL::Input

  class Array < Base

    # Construct reader with array of hashes that we feed back
    def initialize(data)
      super()
      @data = data
    end

    # Regurgitates data from array passed on construction
    def each_row
      @rows_processed = 0
      @data.each do |h|
        h = h.clone
        transform_row!(h)
        yield h
        @rows_processed += 1
      end
    end
  end
end
