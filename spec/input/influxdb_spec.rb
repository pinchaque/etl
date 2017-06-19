require 'influxdb'

require 'etl/core'

RSpec.describe "influxdb inputs", skip: true do
  
  let (:dbconfig) {
    { 
      :port     => 8086,
      :host     => "localhost",
      :database => "test"
    } 
  }
  let(:iql) { '' }
  let(:idb) { ETL::Input::Influxdb.new(dbconfig, iql) }
  let(:ts) { Time.parse('2015-01-10T23:00:50Z').utc } # changing this will break tests
  let(:series) { 'input_test' }
  let(:container) { 'influx_input_test' }

  before(:all) do
    system("docker run -d -t -p 8086:8086 --name influx_input_test influxdb:1.2")

    sleep(0.5) # Give things a second to spin up.

    system("curl -X POST http://localhost:8086/query --data-urlencode \"q=CREATE DATABASE test\"")
  end
  
  before do

    c = idb.conn

    data = []

    for i in 1..1000 do
      h = Hash.new
      h[:series] = series
      h[:timestamp] = ts.to_i + 20*(i-1) 
      h[:values] = { value: i, n: i*2 }
      color = if i%2 == 1
                'red'
              else
                'blue'
              end
      h[:tags] = { foo: 'bar', color: color }
      data.push(h)
    end

    c.create_database(dbconfig[:database])
    c.write_points(data)
    # > select * from input_test
    # name: input_test
    # ----------------
    # time			            color	foo	n	value
    # 2015-01-10T23:00:50Z	red  	bar	2	1
    # 2015-01-10T23:01:10Z	red	  bar	4	2
    # 2015-01-10T23:01:30Z	blue	bar	6	3
  end
  
  after(:all) do
    system("docker stop influx_input_test")
    system("docker rm influx_input_test")
  end

  describe 'dummy parameters' do
  
    it 'provides data source name' do
      p = {
        port: 8086,
        host: '127.0.0.1',
        database: 'metrics',
        username: 'test_user',
        password: 'xyz',
      }
      i = ETL::Input::Influxdb.new(p)
      expect(i.name).to eq("influxdb://test_user@127.0.0.1/metrics")
    end
  end
  
  describe 'test database - first two rows' do
    let(:iql) { "select * from #{series}" }
    
    it 'returns correct rows' do
      rows = []
      idb.each_row { |row| rows << row }
      expect(idb.rows_processed).to eq(1000)

      expected = []

      for i in 1..2 do
        h = Hash.new
        h["time"] = (ts + (i-1)*20).strftime('%FT%TZ') 
        color = if i%2 == 1
                  'red'
                else
                  'blue'
                end
        h["color"] = color
        h["foo"] = "bar" 
        h["n"] = i*2 
        h["value"] = i
        expected.push(h)
      end
      
      expect(rows[0..1]).to eq(expected)
    end
  end
  
  describe 'test database - aggregated by minute' do
    let(:iql) { 
      "select sum(value) as v, count(n) as n from #{series} where time > '2015-01-10T23:00:00Z' and time < '2015-01-10T23:03:00Z' group by time(1m)"
    }
    
    it 'returns correct rows' do
      rows = []
      idb.each_row { |row| rows << row }
      expect(idb.rows_processed).to eq(3)
      
      expected = [
        {
          "time" => "2015-01-10T23:00:00Z",
          "v" => 1,
          "n" => 1,
        },
        {
          "time" => "2015-01-10T23:01:00Z",
          "v" => 9,
          "n" => 3,
        },
        {
          "time" => "2015-01-10T23:02:00Z",
          "v" => 18,
          "n" => 3,
        },
      ]
      
      expect(rows).to eq(expected)
    end
  end
  
  describe 'test database - aggregated by color' do
    let(:iql) { 
      "select sum(value) as v, sum(n) as n from #{series} where time >= '2015-01-10T23:00:50Z' and time < '2015-01-25T23:00:00Z' group by color"
    }
    # influx gets a bit weird here - we don't ask for a time but it gives us one
    # anyway. we don't have a good way of deciding whether or not the user
    # has given us a time w/o parsing the query, which I want to stay away
    # from for the moment. So we accept that time is in the output. it gets
    # set to the lowest possible time given the range.
    
    it 'returns correct rows' do
      rows = []
      idb.each_row { |row| rows << row }

      expect(idb.rows_processed).to eq(2)
      
      expected = [
        {
          "color" => "blue",
          "n" => 501000,
          "time" => "2015-01-10T23:00:50Z",
          "v" => 250500,
        },
        {
          "color" => "red",
          "n" => 500000,
          "time" => "2015-01-10T23:00:50Z",
          "v" => 250000,
        },
      ]
      
      # not sure if influx enforces consistent ordering on these. just sort
      # by color to be safe
      rows.sort! { |a, b| a["color"] <=> b["color"] }
      sort_rows = rows.map { |row| row.sort.to_h }
      expect(sort_rows).to eq(expected)
    end
  end
end
