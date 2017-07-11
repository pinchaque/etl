#require 'etl/redshift/client'
require 'etl/core'

RSpec.describe "redshift", skip: true do
  context "client testing" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test]) }
    it "connect and create and delete a table" do
      table_name = "test_table_1"
      client.drop_table(table_name)

      # TODO: Move create table to when that's
      # implemented later when a new redshift table
      # class has been implemented.
      sql = <<SQL
  create table #{table_name} (
    day timestamp);
SQL
      client.execute(sql)
      client.drop_table(table_name)
    end
    it "get table columns" do
      client.drop_table("test_table1")
      sql = <<SQL
  create table test_table1 (
    day timestamp);
SQL
      client.execute(sql)
      r = client.columns("test_table1")
      expect(r.length).to eq(1)
      expect(r[0][:column]).to eq("day")
      expect(r[0][:type]).to eq("timestamp without time zone")
    end
  end
end
