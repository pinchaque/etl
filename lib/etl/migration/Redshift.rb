require 'sequel'
require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Redshift 
    def initialize
      conn_params = ETL.config.redshift.fetch(:etl, { host: "localhost", port: 5439, user: "masteruser", password: "password" })
      @client = ::ETL::Redshift::Client.new(conn_params) 
    end

    def up
      ""
    end

    def down
      ""
    end
  end
end