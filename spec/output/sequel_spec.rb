require 'etl/core'

def rspec_conn_params
  dbconfig = ETL.config.db[:test]
  dbconfig[:adapter] = 'postgres'
  if dbconfig[:user].nil?
    dbconfig[:user] = dbconfig[:username]
  end
  dbconfig
end

def get_today
  Time.now.utc.strftime("%F %T")
end

class RelDb1 < ETL::Output::Sequel
  def initialize
    super(:upsert, rspec_conn_params)
    define_schema do |s|
      s.date(:day)
    end
  end

  def default_schema
    nil
  end
end

# Test loading into postgres
class TestSequelCreate1 < ETL::Output::Sequel
  def initialize(input)
    super(:insert_append, rspec_conn_params)
    @dest_table = "test_1"
    @reader = input

    define_schema do |s|
      s.date("day")
      s.string("condition")
      s.int("value_int")
      s.numeric("value_num", 10, 1)
      s.float("value_float")
    end
  end

  def default_schema
    nil
  end
end

class TestSequelLoad1 < ETL::Output::Sequel
  def initialize(input, load_strategy, table_name)
    super(load_strategy, rspec_conn_params)
    @dest_table = table_name
    @reader = input
    define_schema do |s|
      s.date(:day)
      s.int(:id)
      s.int(:value)
      s.date(:dw_created)
      s.date(:dw_updated)
      s.primary_key = :id
    end
  end

  def default_schema
    nil
  end
end

# tests multi-column partitions
class TestSequelPartition1 < ETL::Output::Sequel
  def initialize(input, load_strategy, table_name)
    super(load_strategy, rspec_conn_params)
    @dest_table = table_name
    @reader = input
    define_schema do |s|
      s.date(:day)
      s.string(:city_name)
      s.int(:value)
      s.partition_columns = {"day" => "day", "city" => "city_name" }
    end
  end

  def default_schema
    nil
  end
end

RSpec.describe "sequel output" do

  def get_conn
    Sequel.connect(rspec_conn_params)
  end

  # helper function for comparing expected and actual results from PG
  def compare_db_results(e, sequel_result, debug = false)
    results = sequel_result.all

    if (debug)
      puts("Expected:")
      puts(e.length)
      p(e)
      puts("Actual:")
      puts(results.length)
      p(results)
    end
    expect(results.length).to eq(e.length)
    (0...e.length).each do |i|
      a = results[i].values
      expect(a.length).to eq(e[i].length)
      (0...e[i].length).each do |j|
        expect(a[j]).to eq(e[i][j])
      end
    end
  end

  describe "temp table name" do
    chars_max = 64
    names = []
    names << ETL::Output::Sequel.temp_table_name('abcd', "20150501_orgname")
    names << ETL::Output::Sequel.temp_table_name('abcd', "20150501123456_orgnamethatssuperduperlonglong")
    names << ETL::Output::Sequel.temp_table_name('plugin_field_mappings', "20150803_salesforcefreshtest")
    names << ETL::Output::Sequel.temp_table_name('plugin_field_mappings_table_name_is_really_really_very_very_very_long', "20150803_salesforcefreshtest_also_very_very_long")

    names.each do |n|
      it n do
        expect(n.length).to be <= chars_max
      end
    end
  end

  # test out our formatting of values
  describe "value formatting" do
    d = [
      {type: :int, value: 1, expected: "1"},
      {type: :float, value: 1.2, expected: "1.2"},
      {type: :numeric, value: 1.3, expected: "1.3"},
      {type: :string, value: "hello", expected: "'hello'"},
      {type: :blah, value: "hello", expected: "'hello'"},

      {type: :int, value: nil, expected: "null"},
      {type: :float, value: nil, expected: "null"},
      {type: :numeric, value: nil, expected: "null"},
      {type: :string, value: nil, expected: "null"},
      {type: :blah, value: nil, expected: "null"},
    ]

    d.each do |h|
      it "formats values for type #{h[:type]} value #{h[:value]}" do
        col = ETL::Schema::Column.new(h[:type])
        actual = RelDb1.new.value_to_db_str(col, h[:value])
        expect(actual).to eq(h[:expected])
      end
    end
  end

  it "postgres - insert from csv" do
    conn = get_conn()

    # Create destination table
    sql = <<SQL
drop table if exists test_1;
create table test_1 (
  day timestamp,
  condition varchar,
  value_int int,
  value_num numeric(10, 1),
  value_float float);
SQL
    conn.run(sql)


    batch = ETL::Batch.new({ :day => "2015-03-31" })
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")
    input.headers_map = {
        "attribute" => "condition",
        "value_numeric" => "value_num"
    }
    job = TestSequelCreate1.new(input)
    job.row_slice_size = 2 # test batching of rows loaded to tmp

    job.batch = batch
    jr = job.run

    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    result = conn.fetch("select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day, condition from test_1 order by day asc")

    exp_values = [
      ["2015-04-01 00:00:00", "rain"],
      ["2015-04-02 00:00:00", "snow"],
      ["2015-04-03 00:00:00", "sun"],
    ]
    compare_db_results(exp_values, result)
  end

  # Helper to initialize database connection and create table
  def init_conn_table(table_name)
    conn = get_conn()

    # Create destination table
    sql = <<SQL
drop table if exists #{table_name};
create table #{table_name} (
  day timestamp,
  id int,
  value int,
  dw_created timestamp,
  dw_updated timestamp
  );
SQL
    conn.run(sql)
    return conn
  end

  # Test errors, possibly time loc issue
  it "postgres - insert append", skip: true do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-03" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-02", "id" => 11, "value" => 2},
      { "day" => "2015-04-03", "id" => 12, "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"
    conn.run("update #{table_name} set dw_created = '#{d1}', dw_updated = '#{d2}';")

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(2)
    expect(jr.rows_processed).to eq(2)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name}
order by day, id, value;
SQL
    )

    today = get_today

    exp_values = [
      ["2015-04-01 00:00:00", 10, 1, d1, d2],
      ["2015-04-02 00:00:00", 11, 2, d1, d2],
      ["2015-04-02 00:00:00", 11, 4, today, today],
      ["2015-04-02 00:00:00", 13, 5, today, today],
      ["2015-04-03 00:00:00", 12, 3, d1, d2],
    ]
    compare_db_results(exp_values, result)
  end

  # Test errors, possibly time loc issue
  it "postgres - insert table", skip: true do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-03" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-02", "id" => 11, "value" => 2},
      { "day" => "2015-04-03", "id" => 12, "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_table, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"
    conn.run("update #{table_name} set dw_created = '#{d1}', dw_updated = '#{d2}';")

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_table, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(2)
    expect(jr.rows_processed).to eq(2)



    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name}
order by day, id, value;
SQL
    )

    today = get_today
    exp_values = [
      ["2015-04-02 00:00:00", 11, 4, today, today],
      ["2015-04-02 00:00:00", 13, 5, today, today],
    ]

    compare_db_results(exp_values, result)
  end

  # Test errors, possibly time loc issue
  it "postgres - insert partition", skip: true do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-02", "id" => 11, "value" => 2},
      { "day" => "2015-04-03", "id" => 12, "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_partition, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"
    conn.run("update #{table_name} set dw_created = '#{d1}', dw_updated = '#{d2}';")

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
      { "day" => "2015-04-03", "id" => 12, "value" => 6},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_partition, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name}
order by day, id, value;
SQL
    )

    today = get_today
    exp_values = [
      ["2015-04-01 00:00:00", 10, 1, d1, d2],
      ["2015-04-02 00:00:00", 11, 4, today, today],
      ["2015-04-02 00:00:00", 13, 5, today, today],
      ["2015-04-03 00:00:00", 12, 3, d1, d2],
      ["2015-04-03 00:00:00", 12, 6, today, today],
    ]

    compare_db_results(exp_values, result)
  end


  it "postgres - update", skip: true do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-02", "id" => 11, "value" => 2},
      { "day" => "2015-04-03", "id" => 12, "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"
    conn.run("update #{table_name} set dw_created = '#{d1}', dw_updated = '#{d2}';")

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
      { "day" => "2015-04-05", "id" => 12, "value" => 6},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :update, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name}
order by day, id, value;
SQL
    )

    today = get_today
    exp_values = [
      ["2015-04-01 00:00:00", 10, 1, d1, d2],
      ["2015-04-02 00:00:00", 11, 4, d1, today],
      ["2015-04-05 00:00:00", 12, 6, d1, today],
    ]

    compare_db_results(exp_values, result)
  end

  it "postgres - upsert", skip: true do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-02", "id" => 11, "value" => 2},
      { "day" => "2015-04-03", "id" => 12, "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"
    conn.run("update #{table_name} set dw_created = '#{d1}', dw_updated = '#{d2}';")

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
      { "day" => "2015-04-05", "id" => 12, "value" => 6},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :upsert, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name}
order by id, day, value
SQL
    )

    today = get_today
    exp_values = [
      ["2015-04-01 00:00:00", 10, 1, d1, d2],
      ["2015-04-02 00:00:00", 11, 4, d1, today],
      ["2015-04-05 00:00:00", 12, 6, d1, today],
      ["2015-04-02 00:00:00", 13, 5, today, today],
    ]

    compare_db_results(exp_values, result)
  end

  it "postgres - upsert with multiple PK" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })
    data = [
      { "day" => "2015-04-01", "id" => 10, "value" => 1},
      { "day" => "2015-04-01", "id" => 11, "value" => 2},
      { "day" => "2015-04-02", "id" => 11, "value" => 3},
      { "day" => "2015-04-02", "id" => 12, "value" => 4},
      { "day" => "2015-04-02", "id" => 13, "value" => 5},
      { "day" => "2015-04-03", "id" => 10, "value" => 6},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :upsert, table_name)
    job.schema.primary_key = [:day, :id]
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(6)
    expect(jr.rows_processed).to eq(6)

    data = [
      { "day" => "2015-04-02", "id" => 11, "value" => 10},
      { "day" => "2015-04-02", "id" => 14, "value" => 11},
      { "day" => "2015-04-03", "id" => 11, "value" => 12},
      { "day" => "2015-04-04", "id" => 11, "value" => 13},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelLoad1.new(input, :upsert, table_name)
    job.schema.primary_key = [:day, :id]
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(4)
    expect(jr.rows_processed).to eq(4)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
from #{table_name}
order by day, id, value
SQL
    )

    exp_values = [
      ["2015-04-01 00:00:00", 10, 1],
      ["2015-04-01 00:00:00", 11, 2],
      ["2015-04-02 00:00:00", 11, 10],
      ["2015-04-02 00:00:00", 12, 4],
      ["2015-04-02 00:00:00", 13, 5],
      ["2015-04-02 00:00:00", 14, 11],
      ["2015-04-03 00:00:00", 10, 6],
      ["2015-04-03 00:00:00", 11, 12],
      ["2015-04-04 00:00:00", 11, 13],
    ]

    compare_db_results(exp_values, result)
  end


  it "postgres - insert partition multi-column" do
    table_name = "test_3"

    conn = get_conn()
    # Create destination table
    sql = <<SQL
drop table if exists #{table_name};
create table #{table_name} (day timestamp, city_name varchar, value int);
SQL
    conn.run(sql)

    # Initial data fill
    batch = ETL::Batch.new({ :day => "2015-04-02", :city => "Seattle" })
    data = [
      { "day" => "2015-04-01", "city_name" => "Seattle", "value" => 1},
      { "day" => "2015-04-02", "city_name" => "Portland", "value" => 2},
      { "day" => "2015-04-03", "city_name" => "Seattle", "value" => 3},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelPartition1.new(input, :insert_partition, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(3)
    expect(jr.rows_processed).to eq(3)

    # run with partition 2014-04-02, Portland
    batch = ETL::Batch.new({ :day => "2015-04-02", :city => "Portland" })
    data = [
      { "day" => "2015-04-02", "city_name" => "Seattle", "value" => 4},
      { "day" => "2015-04-02", "city_name" => "Portland", "value" => 5},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelPartition1.new(input, :insert_partition, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(2)
    expect(jr.rows_processed).to eq(2)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day, city_name as city, value
from #{table_name}
order by day, city, value;
SQL
    )

    exp_values = [
      ["2015-04-01 00:00:00", "Seattle", 1],
      ["2015-04-02 00:00:00", "Portland", 5],
      ["2015-04-02 00:00:00", "Seattle", 4],
      ["2015-04-03 00:00:00", "Seattle", 3],
    ]
    compare_db_results(exp_values, result)

    # run with partition 2014-04-01, Seattle
    batch = ETL::Batch.new({ :day => "2015-04-01", :city => "Seattle" })
    data = [
      { "day" => "2015-04-01", "city_name" => "Seattle", "value" => 4},
      { "day" => "2015-04-01", "city_name" => "Seattle", "value" => 5},
    ]
    input = ETL::Input::Array.new(data)
    job = TestSequelPartition1.new(input, :insert_partition, table_name)
    job.batch = batch
    jr = job.run
    expect(input.rows_processed).to eq(2)
    expect(jr.rows_processed).to eq(2)

    result = conn.fetch(<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day, city_name as city, value
from #{table_name}
order by day, city, value;
SQL
    )

    exp_values = [
      ["2015-04-01 00:00:00", "Seattle", 4],
      ["2015-04-01 00:00:00", "Seattle", 5],
      ["2015-04-02 00:00:00", "Portland", 5],
      ["2015-04-02 00:00:00", "Seattle", 4],
      ["2015-04-03 00:00:00", "Seattle", 3],
    ]
    compare_db_results(exp_values, result)
  end
end
