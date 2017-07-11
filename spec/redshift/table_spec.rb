require 'etl/redshift/table'

RSpec.describe "Redshift Table" do
  describe "Use Redshift altered Postgres sql lib" do
    context "Can generate sql for schema operations for a table." do
      it "Create a table sql with DIST_STYLE ALL" do
        t = ETL::Redshift::Table.new("test_table", { dist_style:"ALL"})
        t.int("id")
        t.add_primarykey("id")
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int, PRIMARY KEY(id) ) diststyle ALL")
      end
      it "Create a table sql with dist and sort key" do
        t = ETL::Redshift::Table.new("test_table")
        t.int("id")
        t.add_primarykey("id")
        t.set_distkey("id")
        t.add_sortkey("id")
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int, PRIMARY KEY(id) ) DISTKEY(id) SORTKEY(id)")
      end
      it "Create a temp table sql" do
        t = ETL::Redshift::Table.new("test_table", { temp: true})
        t.int("id")
        t.add_primarykey("id")
        expect(t.create_table_sql).to eq("CREATE TEMPORARY TABLE IF NOT EXISTS test_table( \"id\" int, PRIMARY KEY(id) )")
      end
      it "Create a table like another table" do
        t = ETL::Redshift::Table.new("test_table", { like: "other_table"})
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table ( LIKE other_table )")
      end
      it "Create a table that doesn't back up" do
        t = ETL::Redshift::Table.new("test_table", { backup: false})
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table BACKUP NO")
      end
    end
  end
end
