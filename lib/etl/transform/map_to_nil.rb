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

  # Maps the specified values to nil, which is useful for input sources
  # such as CSV files that don't natively support nil.
  class MapToNil < Base

    def initialize(*values)
      super()
      @values = {}
      values.to_a.each do |v|
        @values[v] = 1
      end
    end

    def transform(value)
      @values.has_key?(value) ? nil : value
    end
  end
end
