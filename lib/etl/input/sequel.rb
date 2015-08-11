require 'etl/input/base.rb'
require 'sequel'


module ETL::Input

  # Input class that uses Sequel connection for accessing data. Currently it
  # just supports raw SQL with query param replacement.
  # https://github.com/jeremyevans/sequel
  class Sequel < Base

    # Construct reader based on Sequel connection and SQL query
    def initialize(conn, sql, params = [])
      super()
      @conn = conn
      @sql = sql
      @params = params
    end
    
    # Display connection string for this input
    # TODO: Add table name to this - easier if we're given a Sequel dataset
    def name
      o = @conn.opts
      "Sequel #{o[:adapter]}:#{o[:user]}@#{o[:host]}/#{o[:database]}"
    end

    # Reads each row from the query and passes it to the specified block.
    def each_row
      msg = "Executing Sequel query #{@sql}"
      msg += " with params #{@params.join(", ")}" unless @params.empty?
      ETL.logger.debug(msg)
      @rows_processed = 0
      @conn.fetch(@sql, *@params) do |row_in|
        row = {}
        
        # Sequel returns columns as symbols so we need to translate to strings
        row_in.each do |k, v|
          row[k.to_s] = v
        end
        
        transform_row!(row)
        yield row
        @rows_processed += 1
      end
    end
  end
end
