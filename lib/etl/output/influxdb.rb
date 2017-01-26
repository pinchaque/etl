require 'etl/util/influxdb_conn'

module ETL::Output

  # Class that writes data into InfluxDB
  class Influxdb < Base
    include ETL::InfluxdbConn
    
    attr_accessor :series, :params, :row_slice_size, :ts_column, :ts_precision,
      :empty_tag, :ts_tag_format
        
    # Initialize given connection parameters and series name
    def initialize(params, series = nil)
      super()
      @params = params
      @series = series # must be set eventually
      @row_slice_size = 1000 # rows to write at a time to db
      @ts_column = "time" # column to use for our timestamp
      @ts_precision = "n" # precision=[n,u,ms,s,m,h]
      @empty_tag = "[empty]" # influx doesn't allow empty tags, use this default
      @ts_tag_format = "%F" # how we format timestamp values in tags
      @conn = nil
    end

    # Perform transformation operation on each row that is read. 
    def transform_row(row)
      row
    end

    # Returns the default schema based on the table in the destination db
    def default_schema
      # TODO: add parsing of available tags and measurements to put together
      # a default schema
      nil
    end
    
    # Runs the ETL job
    def run_internal
      log.debug("Loading in slices of #{@row_slice_size} rows")
      reader.each_row_slice(@row_slice_size, @batch) do |rows|
        load_slice(rows)
      end
      msg = "Processed #{reader.rows_processed} input rows for #{@series}"

      ETL::Job::Result.success(reader.rows_processed, msg)
    end

    # Load a single slice of rows (passed in as array) into the db
    def load_slice(input_rows)
      log.debug("Processing slice size #{input_rows.length}")
      
      # create this slice of points we're writing
      points = input_rows.map do |row_in|
        # Read our input row into a hash containing all schema columns
        row_out = read_input_row(row_in) 
        
        # Perform row-level transform
        row_out = transform_row(row_out)
        
        # Store the values and remember which columns we saw
        row_to_point(row_out)
      end
      
      log.debug(points)
      conn.write_points(points, 'n') unless points.empty?
    end
      
    def row_to_point(row)
      raise "Series name not set" unless @series
      timestamp = Time.now
      i_values = {}
      i_tags = {}
      
      # put everything in row into either values or tags based on what we
      # can learn about it from the schema and its type
      row.each do |k, v|
        if k == @ts_column
          timestamp = v.is_a?(Time) ? v : Time.parse(v)
        elsif schema.columns.has_key?(k)
          case schema.columns[k].type
          when :int, :float, :numeric
            i_values[k] = Float(v)
          when :date
            i_tags[k] = Time.parse(v.to_s).utc.strftime(@ts_tag_format)
          else
            i_tags[k] = v.to_s
          end
        elsif is_numeric?(v)
          i_values[k] = Float(v)
        else
          i_tags[k] = v.to_s
        end
      end
      
      # format our tags to be what influxdb expects
      tags = {}
      i_tags.each do |k, v|
        v = @empty_tag if v.nil? || v.empty?
        tags[tag_key(k)] = tag_value(v)
      end
      
      # format our values
      values = {}
      i_values.each do |k, v|
        next if v.nil?
        values[tag_key(k)] = v
      end
      values["value"] = 1.0 if values.empty? # ensure we got something
      
      {
        series: @series,
        values: values,
        tags: tags,
        timestamp: format_timestamp(timestamp.utc),
      }
    end

    # returns nanosecond timestamp that we can write to influxdb
    # this is independent of the precision we've chosen - it's just the
    # format used by API
    def format_timestamp(t)
      t.strftime('%s%9N')
    end
    
    # converts string to tag format which means snake case
    def tag_key(v)
      v.to_s.downcase. # make lowercase
        gsub(/_/, ' '). # convert all underscores to spaces so we can elide
        gsub(/'/, ''). # elide apostrophes
        gsub(/\W/, ' '). # change all non-word chars to spaces
        gsub(/\s+/, '_'). # change multiple whitespace to single underscore
        gsub(/_+$/, ''). # remove trailing underscores
        gsub(/^_+/, ''). # remove leading underscores
        strip
    end

    # normalizes string into something appropriate for a tag value
    def tag_value(v)
      v.to_s.downcase. # make lowercase
        gsub(/\s+/, ' '). # elide consecutive spaces
        strip
    end
    
    # used to determine if the row values are numeric
    def is_numeric?(x)
      return false if x.is_a?(Time) # we want to treat times as dimensions
      return false if x.is_a?(Complex) # not all complex can be converted to float
      return true if x.is_a?(Numeric)
      true if Float(x) rescue false
    end
  end
end
