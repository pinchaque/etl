require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/core'

RSpec.describe "redshift" do
  context "client testing" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test]) }
    it "connect and create and delete a table" do
      table_name = "test_table_1"
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name, { backup: false, dist_style: 'All'})
      table.string("name")
      table.int("id")
      table.add_primarykey("id")

      client.create_table(table)
      client.drop_table(table_name)
    end

    it "get table columns" do
      client.drop_table("test_table1")
      sql = <<SQL
  create table test_table1 (
    day timestamp);
SQL
      client.execute(sql)
      rows = []
      r = client.columns("test_table1").each do |r|
        rows << r
      end
      expect(rows.to_s).to eq("[{\"column\"=>\"day\", \"type\"=>\"timestamp without time zone\"}]")
    end
  end
end
