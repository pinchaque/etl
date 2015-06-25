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

        # Hashes are in the right format
        if row_in.respond_to?(:to_hash)
          yield row_in.to_hash

        # if the CSV row is an array then that means we don't have headers
        # for it and we should turn it into a hash using array indexes
        # as the keys
        elsif row_in.respond_to?(:to_a)
          h = {}
          ary = row_in.to_a
          ary.each_index do |i|
            h[i] = ary[i]
          end
          yield h

        # Unrecognized format
        else
          raise "Input row class #{row_in.class} needs to be a hash or array"
        end

        @rows_processed += 1
      end
    end
  end
end
