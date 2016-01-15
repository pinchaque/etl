require 'tempfile'
require 'fileutils'
require 'csv'

module ETL::Input

  class CSV < Base

    attr_accessor :headers, :headers_map

    # Default params to use for CSV reading
    def default_params
      {
        headers: true,
        col_sep: ",",
        row_sep: "\n",
        quote_char: '"'
      }
    end

    # Params we want to force to be set
    # - We never want headers to be returned since all rows will be treated
    #   as data
    def force_params
      {
        return_headers: false,
      }
    end

    # Construct reader based on file name and options
    # Options are the same as would be passed to the standard CSV class
    def initialize(params = {})
      p = default_params.merge(params).merge(force_params)
      super(p)
      @headers = nil
      @headers_map = {}
    end
    
    # File name we're reading from, taken from parameters
    def file_name
      @params[:file]
    end
    
    # Options we pass to CSV object - everything that was passed in minus our
    # file name
    def csv_options
      @params.reject { |k, v| k == :file }
    end
    
    def name
      "CSV file '#{file_name}'"
    end

    # Reads each row from the input file and passes it to the specified
    # block.
    def each_row
      log.debug("Reading from CSV input file #{file_name}")
      @rows_processed = 0
      ::CSV.foreach(file_name, csv_options) do |row_in|
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
            raise ETL::InputError, "Input row class #{row_in.class} needs to be a hash or array"
          end
        # if we were given the headers to use then we just need to grab the
        # values out of whatever we have
        else
          values = row_in.kind_of?(::CSV::Row) ? row_in.fields : row_in.to_a

          if headers.length != values.length
            raise ETL::InputError, "Must have the same number of headers #{headers.length} " + 
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
            raise ETL::InputError, "Input row does not have expected column '#{name}'"
          end
        end

        transform_row!(row)
        yield row
        @rows_processed += 1
      end
    end
  end
end
