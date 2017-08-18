require 'etl/redshift/batch_factory'
require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/core'

module RedshiftHelpers
  def create_table(conn_params, table_name, &_block)
      puts conn_params.inspect
      client = ETL::Redshift::Client.new(conn_params)
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name)
      table.date("date")
      table.int("id")
      table.add_primarykey("id")
      client.create_table(table)
      client.execute("INSERT INTO #{table_name} (date, id) VALUES ('2010-12-15', 1);")
      begin
        yield
      ensure
        client.drop_table(table_name)
      end
  end
end

RSpec.configure do |c|
  c.include RedshiftHelpers
end

RSpec.describe "redshift_batch_factory", skip: true do
  context "generate" do
    let(:table_name) { "test_table" }
    let(:conn_params) { ETL.config.redshift[:test]}
    it "Start Time from generated batch equal to value in db" do
      create_table(conn_params, table_name) do
        bf = ::ETL::Redshift::BatchFactory.new("SELECT date from #{table_name} LIMIT 1", 3, conn_params)
        b = bf.generate
        expect(b.start_time.to_s).to eq("2010-12-15 08:00:00 UTC")
      end
    end
    it "Start Time from generated batch equal days now - backfill days" do
      create_table(conn_params, table_name) do
        backfill_days = 3
        bf = ::ETL::Redshift::BatchFactory.new("SELECT date from #{table_name} WHERE id != 1 LIMIT 1", backfill_days, conn_params)
        b = bf.generate    
        expected_start_time = Time.now.getutc - 60*60*24*backfill_days
        expect(b.start_time.to_s).to eq(expected_start_time.to_s)
      end
    end
  end
end

