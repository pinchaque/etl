require 'etl/util/influxdb_conn'
require 'etl/query/sequel'

module ETL::Input

  # Input class that uses InfluxDB connection for accessing data
  # Influx doc: https://docs.influxdata.com/influxdb/v0.9/
  # Client lib: https://github.com/influxdata/influxdb-ruby
  class MultiInfluxdb < Base
  # Input class that uses InfluxDB connection for accessing data
  # Influx doc: https://docs.influxdata.com/influxdb/v0.9/
  # Client lib: https://github.com/influxdata/influxdb-ruby
    include ETL::InfluxdbConn
    
    attr_accessor :params

    def initialize(params, select, series, **keyword_args) 
      super()
      @select = select
      @series = series
      @where = keyword_args[:where] if keyword_args.include?(:where)
      @group_by = keyword_args[:group_by] if keyword_args.include?(:group_by)
      @limit = keyword_args[:limit] if keyword_args.include?(:limit)
      @conn = nil
      @params = params
      @today = Time.now.getutc
      @last_stamp = keyword_args[:last_stamp] if keyword_args.include?(:last_stamp) 
    end

    def last_stamp
      @last_stamp ||= first_timestamp
    end

    def limit
      @limit ||= 10000
    end

    def schema_map
      @schema_map ||= get_schema_map
    end

    def field_keys 
      @field_keys ||= get_field_keys
    end

    def tag_keys 
      @tag_keys ||= get_tag_keys
    end

    def get_schema_map
      schema = field_keys
      tag_keys.each do |tag|
        schema[tag.to_sym] = :string if !schema.keys.include?(tag.to_sym)
      end
      {:time => :date}.merge(schema)
    end

    def get_field_keys 
      query = <<-EOS
        show field keys from #{@series}
EOS
      log.debug("Executing InfluxDB query #{query}")
      row = with_retry { conn.query(query, denormalize: false) } || []
      h = Hash.new
      if !row.nil? && row[0]["values"]
        row[0]["values"].each{ |k,v| h[k.to_sym] = v.to_sym }
      end
      h
    end

    def get_tag_keys 
      query = <<-EOS
        show tag keys from #{@series}
EOS
      log.debug("Executing InfluxDB query #{query}")
      row = with_retry { conn.query(query, denormalize: false) } || []
      if !row.nil? && row[0]["values"]
        return row[0]["values"].flatten(1)
      end
      []
    end

    def first_timestamp 
      if !field_keys.empty?
        measurement = field_keys.keys[0].to_s
        query = <<-EOS
          select first(#{measurement}) from #{@series}
EOS
        log.debug("Executing InfluxDB query #{query}")
        row = with_retry { conn.query(query, denormalize: false) } || []

        if !row.nil? && row[0]["columns"] && row[0]["values"]
          h = Hash[row[0]["columns"].zip(row[0]["values"][0])]
          return Time.parse(h["time"])
        end
      end
      @today - 60*60*24*100
    end
    
    # Display connection string for this input
    def name
      "influxdb://#{@params[:username]}@#{@params[:host]}/#{@params[:database]}"
    end

    def time_range(start_date)
      from_date = (start_date).to_s[0..18].gsub("T", " ")
      to_date = (start_date + (60*60*24)).to_s[0..18].gsub("T", " ")
      "time > '#{from_date}' AND time < '#{to_date}'"
    end
        
    # Reads each row from the query and passes it to the specified block.
    def each_row(batch = ETL::Batch.new)

      # We are expecting a result like:
      # [{"name"=>"time_series_1", "tags"=>{"region"=>"uk"}, "columns"=>["time", "count", "value"], "values"=>[["2015-07-09T09:03:31Z", 32, 0.9673], ["2015-07-09T09:03:49Z", 122, 0.4444]]},
      # {"name"=>"time_series_1", "tags"=>{"region"=>"us"}, "columns"=>["time", "count", "value"], "values"=>[["2015-07-09T09:02:54Z", 55, 0.4343]]}]
      # XXX for now this is all going into memory before we can iterate it.
      # It would be nice to switch this to streaming REST call. 

      start_date = 
        if last_stamp.is_a?(String)
          Time.parse(last_stamp)
        else
          last_stamp
        end

      query_sql = ETL::Query::Sequel.new(@select, @series, @where, @group_by, limit)

      @rows_processed = 0
      rows_count = limit

      while start_date < @today do
        query_sql.append_replaceable_where(time_range(start_date))
        rows = with_retry { conn.query(query_sql.query, denormalize: false) } || [].each
        log.debug("Executing InfluxDB query #{query_sql.query}")

        rows_count = 0
        rows.each do |row_in|
          # use the same set of tags for each value set
          tag_row = row_in["tags"] || {}
          
          # do we have a bunch of values to go with these tags?
          if row_in["values"]
            # iterate over all the value sets
            row_in["values"].each do |va|
              # each value set should zip up to same number of items as the 
              # column labels we got back
              if va.count != row_in["columns"].count
                raise "# of columns (#{row_in["columns"]}) does not match values (#{row_in["values"]})" 
              end
              
              # build our row by combining tags and value columns. note that if
              # they are named the same then tags will get overwritten
              row = tag_row.merge(Hash[row_in["columns"].zip(va)])
              
              # boilerplate processing
              transform_row!(row)
              yield row
              rows_count += 1
            end
          else
            # no values? kinda weird, but process it anyway
            transform_row!(tag_row)
            yield tag_row
            rows_count += 1
          end
        end

        @rows_processed += rows_count

        if rows_count < limit
          start_date += 60*60*24 
          query_sql.offset = nil
        else
          query_sql.offset = limit
        end
      end
    end
  end
end