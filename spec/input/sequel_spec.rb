require 'mysql2'

require 'etl/core'


RSpec.describe "sequel inputs", skip: true do
  it "mysql input each" do
    # add data to the test db
    dbconfig = ETL.config.db[:test_mysql]
    client = Mysql2::Client.new(
      :host => dbconfig[:host],
      :database => dbconfig[:database],
      :username => dbconfig[:username],
      :password => dbconfig[:password],
      :flags => Mysql2::Client::MULTI_STATEMENTS)
    
    table_name = "test_table"
    
    insert_sql = <<SQL
drop table if exists #{table_name};
create table #{table_name} (day timestamp, attribute varchar(100));
insert into #{table_name} (day, attribute) values
  ('2015-04-01', 'rain'),
  ('2015-04-02', 'snow'),
  ('2015-04-03', 'sun');
SQL
    client.query(insert_sql)
    client.store_result while client.next_result


    params = {
      adapter: 'mysql2',
      database: dbconfig[:database],
      user: dbconfig[:username],
      password: dbconfig[:password], 
      host: dbconfig[:host],
    }
    
    sql = "select day, attribute from #{table_name} order by day asc"
    input = ETL::Input::Sequel.new(params, sql)
    expect(input.name).to eq("Sequel mysql2:dw@127.0.0.1/dw_test")

    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq(Time.new(2015, 4, 1))
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq(Time.new(2015, 4, 2))
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq(Time.new(2015, 4, 3))
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)

    # test second iteration through the file
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)
   end
end
