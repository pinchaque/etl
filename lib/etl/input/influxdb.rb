require 'etl/util/influxdb_conn'

module ETL::Input

  # Input class that uses InfluxDB connection for accessing data
  # Influx doc: https://docs.influxdata.com/influxdb/v0.9/
  # Client lib: https://github.com/influxdata/influxdb-ruby
  class Influxdb < Base
    include ETL::InfluxdbConn
    
    attr_accessor :iql, :params

    def initialize(params, iql = nil)
      super()
      @iql = iql
      @conn = nil
      @params = params
    end
    
    # Display connection string for this input
    def name
      "influxdb://#{@params[:username]}@#{@params[:host]}/#{@params[:database]}"
    end
        
    # Reads each row from the query and passes it to the specified block.
    def each_row(batch = ETL::Batch.new)
      log.debug("Executing InfluxDB query #{iql}")
      # We are expecting a result like:
      # [{"name"=>"time_series_1", "tags"=>{"region"=>"uk"}, "columns"=>["time", "count", "value"], "values"=>[["2015-07-09T09:03:31Z", 32, 0.9673], ["2015-07-09T09:03:49Z", 122, 0.4444]]},
      # {"name"=>"time_series_1", "tags"=>{"region"=>"us"}, "columns"=>["time", "count", "value"], "values"=>[["2015-07-09T09:02:54Z", 55, 0.4343]]}]
      # XXX for now this is all going into memory before we can iterate it.
      # It would be nice to switch this to streaming REST call. 
      rows = with_retry { conn.query(iql, denormalize: false) } || [].each
      puts "rows #{rows}"
      
      @rows_processed = 0
      # each row we get back from influx can have multiple value sets assoc
      # with a given tag set
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
            @rows_processed += 1
          end
        else
          # no values? kinda weird, but process it anyway
          transform_row!(tag_row)
          yield tag_row
          @rows_processed += 1
        end
      end
    end
  end
end
