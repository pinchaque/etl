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
require 'mysql2'


module ETL::Input

  class MySQL < Base

    # Construct reader based on query to run
    def initialize(client, sql)
      super()
      @client = client
      @sql = sql
    end

    # Reads each row from the query and passes it to the specified block.
    def each_row
      Rails.logger.debug("Executing MySQL query #{@sql}")
      @rows_processed = 0
      @client.query(@sql).each do |row_in|
        row = row_in.clone
        transform_row!(row)
        yield row
        @rows_processed += 1
      end
    end
  end
end
