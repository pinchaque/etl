require 'influxdb'

module ETL

  # Mixin module that contains helpful functions for accessing InfluxDB
  # connections
  # Influx doc: https://docs.influxdata.com/influxdb/v0.9/
  # Client lib: https://github.com/influxdata/influxdb-ruby
  module InfluxdbConn
    
    # returns influxdb client
    def conn
      # these are the params that influxdb needs
      keys = [:host, :port, :username, :password, :retry, :min_delay, :max_delay]
      con_options = @params.select { |k, v| keys.include?(k) && !v.nil? }
      @conn ||= InfluxDB::Client.new @params[:database], **con_options
    end
    
    protected
    
    # performs block with retries
    def with_retry(&block)
      num_sec = @params[:global_delay_start]
      attempts = 0
      begin
        attempts += 1
        yield
      rescue InfluxDB::Error => ex
        if attempts < @params[:global_max_attempts]
          sleep(num_sec)
          num_sec *= @params[:global_multiplier]
          retry
        end
        raise
      end
    end
  end
end
