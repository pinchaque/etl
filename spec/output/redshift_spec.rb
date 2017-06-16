require 'etl/core'

def rspec_aws_params
  ETL.config.aws[:test]
end

def rspec_redshift_params
  ETL.config.redshift[:test]
end
  
# Test loading into Redshift
class TestRedshiftCreate1 < ETL::Output::Redshift
  def initialize(input, table_name)
    super(:insert_append, rspec_redshift_params, rspec_aws_params) 
    @dest_table = table_name 
    @reader = input
    #@csv_file = "#{ETL.root}/spec/data/simple1.csv"

    define_schema do |s|
      s.int("build_number")
      s.string("name")
      s.int("count")
      s.float("total")
      s.float("average")
    end
  end
  
  def default_schema
    nil
  end
end

class TestRedshiftLoad1 < ETL::Output::Redshift
  def initialize(input, load_strategy, table_name)
    super(load_strategy, rspec_redshift_params, rspec_aws_params)
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

RSpec.describe "redshift output", skip: true do

  def get_conn
    PG.connect(rspec_redshift_params)
  end

  # helper function for comparing expected and actual results from Redshift
  def compare_db_results(e, result, debug = false)
    results = result.values

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
      a = results[i]
      expect(a.length).to eq(e[i].length)
      (0...e[i].length).each do |j|
        expect(a[j]).to eq(e[i][j])
      end
    end
  end

  def upload_file_to_s3(s3key, file)
  	#upload test data to s3
    Aws.config.update({
      region: rspec_aws_params[:region],
      credentials: Aws::Credentials.new(
        rspec_aws_params[:access_key_id],
        rspec_aws_params[:secret_access_key])
    })
    s3 = Aws::S3::Resource.new
    s3.bucket(rspec_aws_params[:s3_bucket]).object(s3key).upload_file(file)
  end

  def upload_string_to_s3(s3key, str)
  	#upload test data to s3
    Aws.config.update({
      region: rspec_aws_params[:region],
      credentials: Aws::Credentials.new(
        rspec_aws_params[:access_key_id],
        rspec_aws_params[:secret_access_key])
    })
    s3 = Aws::S3::Resource.new
    obj = s3.bucket(rspec_aws_params[:s3_bucket]).object(s3key)
    obj.put(body: str)
  end

  def make_csv(file, matrix)
    CSV.open(file, "w") do |c|
      matrix.each_index do |indx|
        c << matrix.fetch(indx)
      end
    end
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
    conn.exec(sql)
    return conn
  end

  it "redshift - insert from s3" do
    table_name = "test_1"
    conn = init_conn_table(table_name)
    
    # Create destination table
    sql = <<SQL
drop table if exists #{table_name};
create table #{table_name} (
  day timestamp, 
  condition varchar, 
  value_int int, 
  value_num numeric(10, 1), 
  value_float float);
SQL
    conn.exec(sql)

    batch = ETL::Batch.new({ :day => "2015-03-31" })

    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")
    job = TestRedshiftCreate1.new(input, table_name)

    job.batch = batch
    jr = job.run

    result = conn.exec("select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day, condition from #{table_name} order by day asc")
    
    exp_values = [
      ["2015-04-01 00:00:00", "rain"],
      ["2015-04-02 00:00:00", "snow"],
      ["2015-04-03 00:00:00", "sun"],
    ]
    compare_db_results(exp_values, result)
  end 

  it "redshift - insert append" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-03" })

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-01", 10, 1, d1, d2])
    data.push(["2015-04-02", 11, 2, d1, d2])
    data.push(["2015-04-03", 12, 3, d1, d2])

    #upload_string_to_s3(table_name, data)
    make_csv('/tmp/test2.txt', data)

    input = ETL::Input::CSV.new('/tmp/test2.txt')
    job = TestRedshiftLoad1.new(input, :insert_append, table_name)

    job.batch = batch
    jr = job.run

    today = DateTime.now.strftime("%F %T")

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-02", 11, 4, today, today])
    data.push(["2015-04-02", 13, 5, today, today])

    make_csv('/tmp/test2.txt', data)

    input = ETL::Input::CSV.new('/tmp/test2.txt')
    job = TestRedshiftLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run

    sql =<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name} 
order by day, id, value;
SQL
    result = conn.exec(sql)

    exp_values = [
      ["2015-04-01 00:00:00", "10", "1", d1, d2],
      ["2015-04-02 00:00:00", "11", "2", d1, d2],
      ["2015-04-02 00:00:00", "11", "4", today, today],
      ["2015-04-02 00:00:00", "13", "5", today, today],
      ["2015-04-03 00:00:00", "12", "3", d1, d2],
    ]
    compare_db_results(exp_values, result)
  end

  it "redshift - insert table" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-01", 10, 1, d1, d2])
    data.push(["2015-04-02", 11, 2, d1, d2])
    data.push(["2015-04-03", 12, 3, d1, d2])

    make_csv('/tmp/test2.txt', data)
    batch = ETL::Batch.new({ :day => "2015-04-03" })
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :insert_table, table_name)
    job.batch = batch
    jr = job.run

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    today = DateTime.now.strftime("%F %T")

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-02", 11, 4, today, today])
    data.push(["2015-04-02", 13, 5, today, today])

    make_csv('/tmp/test2.txt', data)

    input = ETL::Input::CSV.new('/tmp/test2.txt')
    job = TestRedshiftLoad1.new(input, :insert_table, table_name)
    job.batch = batch
    jr = job.run

    sql =<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name} 
order by day, id, value;
SQL
    result = conn.exec(sql)

    exp_values = [
      ["2015-04-02 00:00:00", "11", "4", today, today],
      ["2015-04-02 00:00:00", "13", "5", today, today],
    ]

    compare_db_results(exp_values, result)
  end

  it "redshift - update" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-01", 10, 1, d1, d2])
    data.push(["2015-04-02", 11, 2, d1, d2])
    data.push(["2015-04-03", 12, 3, d1, d2])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    batch = ETL::Batch.new({ :day => "2015-04-02" })

    job = TestRedshiftLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run

    today = DateTime.now.strftime("%F %T")

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-02", 11, 4, today, today])
    data.push(["2015-04-02", 13, 5, today, today])
    data.push(["2015-04-05", 12, 6, today, today])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :update, table_name)
    job.batch = batch
    jr = job.run

    sql =<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name} 
order by day, id, value;
SQL
    result = conn.exec(sql)

    exp_values = [
      ["2015-04-01 00:00:00", "10", "1", d1, d2],
      ["2015-04-02 00:00:00", "11", "4", today, today],
      ["2015-04-05 00:00:00", "12", "6", today, today],
    ]

    compare_db_results(exp_values, result)
  end

  it "redshift - upsert" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-01", 10, 1, d1, d2])
    data.push(["2015-04-02", 11, 2, d1, d2])
    data.push(["2015-04-03", 12, 3, d1, d2])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :insert_append, table_name)
    job.batch = batch
    jr = job.run

    today = DateTime.now.strftime("%F %T")

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-02", 11, 4, today, today])
    data.push(["2015-04-02", 13, 5, today, today])
    data.push(["2015-04-05", 12, 6, today, today])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :upsert, table_name)
    job.batch = batch
    jr = job.run

    sql =<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
  , to_char(dw_created, 'YYYY-MM-DD HH24:MI:SS') as dw_created
  , to_char(dw_updated, 'YYYY-MM-DD HH24:MI:SS') as dw_updated
from #{table_name} 
order by id, day, value
SQL
    result = conn.exec(sql)

    exp_values = [
      ["2015-04-01 00:00:00", "10", "1", d1, d2],
      ["2015-04-02 00:00:00", "11", "4", today, today],
      ["2015-04-05 00:00:00", "12", "6", today, today],
      ["2015-04-02 00:00:00", "13", "5", today, today],
    ]

    compare_db_results(exp_values, result)
  end

  it "redshift - upsert with multiple PK" do
    table_name = "test_2"
    conn = init_conn_table(table_name)

    batch = ETL::Batch.new({ :day => "2015-04-02" })

    d1 = "2015-02-03 12:34:56"
    d2 = "2015-02-04 01:23:45"

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-01", 10, 1, d1, d2])
    data.push(["2015-04-01", 11, 2, d1, d2])
    data.push(["2015-04-02", 11, 3, d1, d2])
    data.push(["2015-04-02", 12, 4, d1, d2])
    data.push(["2015-04-02", 13, 5, d1, d2])
    data.push(["2015-04-03", 10, 6, d1, d2])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :upsert, table_name)
    job.schema.primary_key = [:day, :id]
    job.batch = batch
    jr = job.run

    data = []
    data.push(["day", "id", "value", "dw_created", "dw_updated"])
    data.push(["2015-04-02", 11, 10, d1, d2])
    data.push(["2015-04-02", 14, 11, d1, d2])
    data.push(["2015-04-03", 11, 12, d1, d2])
    data.push(["2015-04-04", 11, 13, d1, d2])

    make_csv('/tmp/test2.txt', data)
    input = ETL::Input::CSV.new('/tmp/test2.txt')

    job = TestRedshiftLoad1.new(input, :upsert, table_name)
    job.schema.primary_key = [:day, :id]
    job.batch = batch
    jr = job.run

    sql =<<SQL
select to_char(day, 'YYYY-MM-DD HH24:MI:SS') as day
  , id
  , value
from #{table_name} 
order by day, id, value
SQL
    result = conn.exec(sql)

    exp_values = [
      ["2015-04-01 00:00:00", "10", "1"],
      ["2015-04-01 00:00:00", "11", "2"],
      ["2015-04-02 00:00:00", "11", "10"],
      ["2015-04-02 00:00:00", "12", "4"],
      ["2015-04-02 00:00:00", "13", "5"],
      ["2015-04-02 00:00:00", "14", "11"],
      ["2015-04-03 00:00:00", "10", "6"],
      ["2015-04-03 00:00:00", "11", "12"],
      ["2015-04-04 00:00:00", "11", "13"],
    ]

    compare_db_results(exp_values, result)
  end
end
