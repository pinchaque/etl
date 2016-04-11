require 'influxdb'

module ETL

  # Mixin module that contains helpful functions for accessing InfluxDB
  # connections
  # Influx doc: https://docs.influxdata.com/influxdb/v0.9/
  # Client lib: https://github.com/influxdata/influxdb-ruby
  module InfluxdbConn
    
    # Default connection parameters we use for InfluxDB
    def default_conn_params
      {
        retry: 2, # limit retries to avoid blocking forever
        min_delay: 0.02,
        max_delay: 0.5,
        port: 8086,
        host: 'localhost',
        database: 'default',
        username: nil,
        password: nil,
        global_max_attempts: 5,
        global_delay_start: 0.5,
        global_multiplier: 2.0,
      }
    end
    
    def conn_params
      @conn_params ||= default_conn_params.merge(params)
    end
    
    # returns influxdb client
    def conn
      # these are the params that influxdb needs
      keys = [:host, :port, :username, :password, :retry, :min_delay, :max_delay]
      con_options = conn_params.select { |k, v| keys.include?(k) && !v.nil? }
      @conn ||= InfluxDB::Client.new conn_params[:database], **con_options
    end
    
    protected
    
    # performs block with retries
    def with_retry(&block)
      num_sec = conn_params[:global_delay_start]
      attempts = 0
      begin
        attempts += 1
        yield
      rescue InfluxDB::Error => ex
        if attempts < conn_params[:global_max_attempts]
          sleep(num_sec)
          num_sec *= conn_params[:global_multiplier]
          retry
        end
        raise
      end
    end
  end
end
