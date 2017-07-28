require 'etl/redshift/table'

RSpec.describe "Redshift Table" do
  describe "Use Redshift altered Postgres sql lib" do
    context "Can generate sql for schema operations for a table." do
      let (:test_table) { "test_table" }

      it "Create a table sql with DIST_STYLE ALL" do
        t = ETL::Redshift::Table.new(:test_table, { dist_style:"ALL" })
        t.int(:id)
        t.add_primarykey(:id)
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int NOT NULL, PRIMARY KEY(id) ) DISTSTYLE ALL")
      end

      it "Create a table sql with dist and sort key" do
        t = ETL::Redshift::Table.new(:test_table)
        t.int(:id)
        t.add_primarykey(:id)
        t.set_distkey(:id)
        t.add_sortkey(:id)
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int NOT NULL, PRIMARY KEY(id) ) DISTKEY(id) SORTKEY(id)")
      end

      it "Create a temp table sql" do
        t = ETL::Redshift::Table.new(:test_table, { temp: true })
        t.int(:id)
        t.add_primarykey(:id)
        expect(t.create_table_sql).to eq("CREATE TEMPORARY TABLE IF NOT EXISTS test_table( \"id\" int NOT NULL, PRIMARY KEY(id) )")
      end

      it "Create a table like another table" do
        t = ETL::Redshift::Table.new(:test_table, { like: "other_table" })
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table ( LIKE other_table )")
      end

      it "Create a table that doesn't back up" do
        t = ETL::Redshift::Table.new(:test_table, { backup: false })
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table BACKUP NO")
      end

      it "Create a table with identity and default parameters" do
        t = ETL::Redshift::Table.new(:test_table)
        t.int(:id)
        t.set_identity(:id)
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int IDENTITY(1, 1) NOT NULL )")
      end

      it "Create a table with identity and custom parameters" do
        t = ETL::Redshift::Table.new(:test_table)
        t.int(:id)
        t.set_identity(:id, 0, 2)
        expect(t.create_table_sql).to eq("CREATE TABLE IF NOT EXISTS test_table( \"id\" int IDENTITY(0, 2) NOT NULL )")
      end
    end
  end
end
