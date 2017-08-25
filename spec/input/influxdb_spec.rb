require 'influxdb'

require 'etl/core'

RSpec.describe "influxdb inputs" do
  
  let(:dbconfig) { ETL.config.db[:influxdb] }
  let(:iql) { '' }
  let(:idb) { ETL::Input::Influxdb.new(dbconfig, iql) }
  let(:ts) { Time.parse('2015-01-10T23:00:50Z').utc } # changing this will break tests
  
  let(:series) { 'input_test' }
  
  before do
    skip "Missing InfluxDB config" unless dbconfig
    c = idb.conn
    
    data = [
      {
        series: series,
        timestamp: ts.to_i,
        values: { value: 1, n: 2 },
        tags: { foo: 'bar', color: 'red' }
      },
      {
        series: series,
        timestamp: ts.to_i + 25,
        values: { value: 2, n: 4 },
        tags: { foo: 'bar', color: 'red' }
      },
      {
        series: series,
        timestamp: ts.to_i + 45,
        values: { value: 3, n: 6 },
        tags: { foo: 'bar', color: 'blue' }
      },
    ]

    c.write_points(data)
    # > select * from input_test
    # name: input_test
    # ----------------
    # time			            color	foo	n	value
    # 2015-01-10T23:00:50Z	red  	bar	2	1
    # 2015-01-10T23:01:15Z	red	  bar	4	2
    # 2015-01-10T23:01:35Z	blue	bar	6	3
  end
  
  after do
    # ideally we'd clean up the data points but we can't do that w/o admin
    # access. the reality is that the test will just keep overwriting the same
    # data so it's not a big problem
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
  
  describe 'test database - all data' do
    let(:iql) { "select * from #{series}" }
    
    it 'returns correct rows' do
      rows = []
      idb.each_row { |row| rows << row }
      expect(idb.rows_processed).to eq(3)
      
      expected = [
        {
          "time" => ts.strftime('%FT%TZ'),
          "color" => "red",
          "foo" => "bar",
          "n" => 2,
          "value" => 1,
        },
        {
          "time" => (ts + 25).strftime('%FT%TZ'),
          "color" => "red",
          "foo" => "bar",
          "n" => 4,
          "value" => 2,
        },
        {
          "time" => (ts + 45).strftime('%FT%TZ'),
          "color" => "blue",
          "foo" => "bar",
          "n" => 6,
          "value" => 3,
        },
      ]
      
      expect(rows).to eq(expected)
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
          "n" => 1,
          "v" => 1,
        },
        {
          "time" => "2015-01-10T23:01:00Z",
          "n" => 2,
          "v" => 5,
        },
        {
          "time" => "2015-01-10T23:02:00Z",
          "n" => 0,
          "v" => nil,
        },
      ]
      
      expect(rows).to eq(expected)
    end
  end
  
  describe 'test database - aggregated by color' do
    let(:iql) { 
      "select sum(value) as v, sum(n) as n from #{series} where time >= '2015-01-10T23:01:00Z' and time < '2015-01-10T23:03:00Z' group by color"
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
          "time" => "2015-01-10T23:01:00Z",
          "color" => "blue",
          "n" => 6,
          "v" => 3,
        },
        {
          "time" => "2015-01-10T23:01:00Z",
          "color" => "red",
          "n" => 4,
          "v" => 2,
        },
      ]
      
      # not sure if influx enforces consistent ordering on these. just sort
      # by color to be safe
      rows.sort! { |a, b| a["color"] <=> b["color"] }
      
      expect(rows).to eq(expected)
    end
  end
end
