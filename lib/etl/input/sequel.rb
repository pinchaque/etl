require 'sequel'

module ETL::Input

  # Input class that uses Sequel connection for accessing data. Currently it
  # just supports raw SQL with query param replacement.
  # https://github.com/jeremyevans/sequel
  class Sequel < Base

    # Construct reader based on Sequel connection and SQL query
    def initialize(params = {})
      super
      @conn = nil
    end
    
    def sql
      @params[:sql]
    end
    
    def sql_params
      @params[:params]
    end
    
    def conn
      @conn ||= ::Sequel.connect(@params)
    end
    
    # Display connection string for this input
    # TODO: Add table name to this - easier if we're given a Sequel dataset
    def name
      "Sequel #{@params[:adapter]}:#{@params[:user]}@#{@params[:host]}/#{@params[:database]}"
    end

    # Reads each row from the query and passes it to the specified block.
    def each_row
      msg = "Executing Sequel query #{sql}"
      unless sql_params.nil? or sql_params.empty?
        if sql_params.respond_to?(:join)
          param_str = sql_params.join(", ")
        elsif 
          param_str = sql_params.to_s
        end
        msg += " with params #{param_str}"
      else
        msg += " with no params"
      end
      ETL.logger.debug(msg)
      
      # block used to process each row
      row_proc = Proc.new do |row_in|
        row = {}
        
        # Sequel returns columns as symbols so we need to translate to strings
        row_in.each do |k, v|
          row[k.to_s] = v
        end
        
        transform_row!(row)
        yield row
        @rows_processed += 1
      end
      
      @rows_processed = 0
      # need to splat differently depending on params type
      if sql_params.is_a?(Hash)
        conn.fetch(sql, **sql_params, &row_proc) 
      else
        conn.fetch(sql, *sql_params, &row_proc) 
      end
    end
  end
end
