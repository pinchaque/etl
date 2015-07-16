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

    attr_accessor :headers, :headers_map

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
      @headers = nil
      @headers_map = {}
    end

    # Reads each row from the input file and passes it to the specified
    # block.
    def each_row
      ETL.logger.debug("Reading from CSV input file #{@file_name}")
      @rows_processed = 0
      ::CSV.foreach(@file_name, @options) do |row_in|
        # Row that maps name => value
        row = {}

        # If we weren't given headers then we use what's in the file
        if headers.nil?
          # We have a hash - OK we'll use it
          if row_in.respond_to?(:to_hash)
            row = row_in.to_hash
          # We have an array - use numbers as the keys
          elsif row_in.respond_to?(:to_a)
            ary = row_in.to_a
            ary.each_index do |i|
              row[i] = ary[i]
            end
          # Error out since we don't know how to process this
          else
            raise "Input row class #{row_in.class} needs to be a hash or array"
          end
        # if we were given the headers to use then we just need to grab the
        # values out of whatever we have
        else
          values = row_in.kind_of?(::CSV::Row) ? row_in.fields : row_in.to_a

          if headers.length != values.length
            raise "Must have the same number of headers #{headers.length} " + 
              "and values #{values.length}"
          end

          # match up headers and values
          (0...headers.length).each do |i|
            row[headers[i]] = values[i]
          end
        end

        # now we apply our header map if we have one
        @headers_map.each do |name, new_name|
          if row.has_key?(name)
            # remap old name to new name
            row[new_name] = row[name]
            row.delete(name)
          else
            raise "Input row does not have expected column '#{name}'"
          end
        end

        transform_row!(row)
        yield row
        @rows_processed += 1
      end
    end
  end
end
