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
require 'sequel'


module ETL::Input

  # Input class that uses Sequel connection for accessing data. Currently it
  # just supports raw SQL with query param replacement.
  # https://github.com/jeremyevans/sequel
  class Sequel < Base

    # Construct reader based on Sequel connection and SQL query
    def initialize(conn, sql, params = [])
      super()
      @conn = conn
      @sql = sql
      @params = params
    end

    # Reads each row from the query and passes it to the specified block.
    def each_row
      Rails.logger.debug("Executing Sequel query #{@sql} with params #{@params.join(", ")}")
      @rows_processed = 0
      @conn.fetch(@sql, *@params) do |row_in|
        row = {}
        
        # Sequel returns columns as symbols so we need to translate to strings
        row_in.each do |k, v|
          row[k.to_s] = v
        end
        
        transform_row!(row)
        yield row
        @rows_processed += 1
      end
    end
  end
end
