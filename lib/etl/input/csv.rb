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
require 'tempfile'
require 'fileutils'
require 'csv'

module ETL::Input

  class CSV < Base

    # Default options to use for CSV reading
    def default_options
      {
        headers: true,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }
    end

    # Options we want to force to be set
    # - We never want headers to be returned since all rows will be treated
    #   as data
    def force_options
      {
        return_headers: false,
      }
    end

    # Construct reader based on file name and options
    # Options are the same as would be passed to the standard CSV class
    def initialize(file_name, options = {})
      super()

      @file_name = file_name
      @options = default_options.merge(options).merge(force_options)
    end

    # Reads each row from the input file and passes it to the specified
    # block.
    def each_row
      Rails.logger.debug("Reading from CSV input file #{@file_name}")
      ::CSV.foreach(@file_name, @options) do |row_in|
        @rows_processed += 1
        yield row_in
      end
    end
  end
end
