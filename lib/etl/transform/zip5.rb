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

  class Zip5 < Base

    # Validates that the specified value is a well-formatted 5-digit zip code
    def initialize
      super()
    end

    # Truncates the date
    def transform(value)
      v = value.clone

      # clean out leading/trailing whitespace
      v.strip!

      # We expect the string to just be digits and hyphen, otherwise this
      # isn't a zip code
      return nil unless v =~ /^\d+(-\d*)?+$/

      # Chop everything off from hyphen onwards
      v.gsub!(/-.*$/, "")

      # Pad left with "0" if needed
      v = v.rjust(5, "0")

      # Return first 5 chars
      v[0..4]
    end
  end
end
