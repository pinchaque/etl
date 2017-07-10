#require 'etl/redshift/client'
require 'etl/core'

RSpec.describe "redshift", skip: true do
  it "Create a new client" do
    conn_params = ETL.config.redshift[:test]
    ::ETL::Redshift::Client.new(conn_params)
  end
  it "connect to an odbc database" do
    conn_params = ETL.config.redshift[:test]
    c = ETL::Redshift::Client.new(conn_params)
    c.connect
    c.execute("select * from pg_table_def")
  end
  it "create/delete table" do
    conn_params = ETL.config.redshift[:test]
    c = ETL::Redshift::Client.new(conn_params)
    c.connect
    c.execute("select * from pg_table_def")
    table_name = "test_table_1"
    c.drop_table(table_name)

    # TODO: Move create table to when that's
    # implemented later when a new redshift table
    # class has been implemented.
    sql = <<SQL
create table #{table_name} (
  day timestamp);
SQL
    c.execute(sql)
    c.drop_table(table_name)
  end
end
