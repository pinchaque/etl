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

require 'etl/transform/base.rb'

module ETL::Transform

  # Truncates date/time strings to the specified precision
  class DateTrunc < Base

    # Construct transform with degree of precision
    def initialize(precision)
      super()
      @precision = precision
    end

    # Truncates the date
    def transform(value)
      value
    end
  end
end
