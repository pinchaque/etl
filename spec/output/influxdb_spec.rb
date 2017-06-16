require 'influxdb'
require 'securerandom'
require 'etl/core'

RSpec.describe "influxdb output", skip: true do
  
  let(:rnd_str) { SecureRandom.hex }
  let(:dbconfig) { ETL.config.db[:influxdb] }
  let(:iql) { '' }
  let(:series) { 'output_test' }
  let(:ts_column) { nil }
  let(:ts_tag_format) { nil }
  let(:empty_tag) { nil }
  let(:odb) { 
    o = ETL::Output::Influxdb.new(dbconfig, series) 
    o.ts_column = ts_column if ts_column
    o.ts_tag_format = ts_tag_format if ts_tag_format
    o.empty_tag = empty_tag if empty_tag
    o
  }
  let(:ts) { Time.parse('2015-01-10T23:00:50Z').utc } # changing this will break tests
  
  let(:input) {
    data = [ { "time" => ts, "label" => rnd_str, "temp" => 66.2 } ]
    ETL::Input::Array.new(data)
  }
  
  describe 'is_numeric' do
    {
      1 => true,
      3.14 => true,
      :symbol => false,
      "a string" => false,
      "34" => true,
      "3.14" => true,
      Time.now => false,
      Complex(2, 3) => false,
      Rational(2, 3) => true,
    }.each do |input, output|
      it input do
        expect(odb.is_numeric?(input)).to (output ? be_truthy : be_falsy)
      end
    end
  end
  
  describe 'tag_key' do
    {
      'Foo' => 'foo',
      'Foo bar' => 'foo_bar',
      'Foo @@bar!!' => 'foo_bar',
      :symbol_time => 'symbol_time',
      "Howard's 3rd car" => 'howards_3rd_car',
      'already_a_tag' => 'already_a_tag',
      'tag_with a space' => 'tag_with_a_space',
      "string with \t\nlots   \rof   ws" => 'string_with_lots_of_ws',
      "email@host.domain.com" => 'email_host_domain_com',
      "  starting/trailing spaces  " => 'starting_trailing_spaces',
    }.each do |input, output|
      it input do
        expect(odb.tag_key(input)).to eq(output)
      end
    end
  end

  describe 'tag_value' do
    {
      'Foo' => 'foo',
      'Foo bar' => 'foo bar',
      'Foo @@bar!!' => 'foo @@bar!!',
      :symbol_time => 'symbol_time',
      "Howard's 3rd car" => "howard's 3rd car",
      'already_a_tag' => 'already_a_tag',
      'tag_with a space' => 'tag_with a space',
      "string with \t\nlots   \rof   ws" => 'string with lots of ws',
      "email@host.domain.com" => 'email@host.domain.com',
      "  starting/trailing spaces  " => 'starting/trailing spaces',
    }.each do |input, output|
      it input do
        expect(odb.tag_value(input)).to eq(output)
      end
    end
  end
  
  describe 'create points without default schema' do
    [
      { # basic input/output
        input: {
          "time" => '2015-01-10T23:00:50Z',
          "color" => "red",
          "foo" => "bar",
          "n" => 2,
          "value" => 3,
        },
        output: {
          series: 'output_test',
          timestamp: Time.parse('2015-01-10T23:00:50Z').utc.strftime("%s%9N"),
          values: { "value" => 3.0, "n" => 2.0 },
          tags: { "foo" => "bar", "color" => "red" },
        },
      },
      { # no measurements
        input: {
          "time" => '2015-01-10T23:00:50Z',
          "color" => "red",
          "foo" => "bar",
        },
        output: {
          series: 'output_test',
          timestamp: Time.parse('2015-01-10T23:00:50Z').utc.strftime("%s%9N"),
          values: { "value" => 1.0 },
          tags: { "foo" => "bar", "color" => "red" },
        },
      },
      { # no tags
        input: {
          "time" => '2015-01-10T23:00:50Z',
          "n" => 2,
          "value" => 3,
        },
        output: {
          series: 'output_test',
          timestamp: Time.parse('2015-01-10T23:00:50Z').utc.strftime("%s%9N"),
          values: { "value" => 3.0, "n" => 2.0 },
          tags: { },
        },
      },
      { # tag and measurement names needing to be taggified
        input: {
          "time" => '2015-01-10T23:00:50Z',
          "wall color" => "red",
          "inside temp!" => 66,
        },
        output: {
          series: 'output_test',
          timestamp: Time.parse('2015-01-10T23:00:50Z').utc.strftime("%s%9N"),
          values: { "inside_temp" => 66.0 },
          tags: { "wall_color" => "red" },
        },
      },
    ].each do |a|
      it a[:input] do
        expect(odb.row_to_point(a[:input])).to eq(a[:output])
      end
    end
  end
  
  describe 'create points using schema' do
    # tests out a bunch of the parameters we can give to the influx outputter
    let(:ts_column) { "ts" }
    let(:ts_tag_format) { "%D" }
    let(:empty_tag) { "null" }
        
        
    [
      {
        input: {
          "ts" => '2015-01-10T23:00:50Z',
          "created" => '2015-01-11T23:00:50Z',
          "color" => "red",
          "foo" => "bar",
          "bar" => nil,
          "n" => 2,
          "value" => 3,
        },
        output: {
          series: 'output_test',
          timestamp: Time.parse('2015-01-10T23:00:50Z').utc.strftime("%s%9N"),
          values: { "value" => 3.0 },
          tags: { 
            "created" => '01/11/15',
            "color" => "red",
            "foo" => "bar", 
            "bar" => "null",
            "n" => "2",
          },
        },
      },
    ].each do |a|
      it a[:input] do
        
        odb.define_schema do |s|
          s.date("day")
          s.date("created") # adds a date
          s.string("color")
          # s.string("foo") not present in schema - yet should be in output
          s.string("bar") # adds empty field
          s.string("n") # converts an integer to a tag
          s.float("value")
        end
        
        expect(odb.row_to_point(a[:input])).to eq(a[:output])
      end
    end
  end
    
  describe 'exceptions' do
    let(:series) { nil }
    it 'raises on missing series' do
      input = {
        "time" => '2015-01-10T23:00:50Z',
        "color" => "red",
        "foo" => "bar",
        "n" => 2,
        "value" => 3,
      }
      expect { odb.row_to_point(input) }.to raise_error("Series name not set")
    end
  end
  
  it 'writes data to influxdb' do
    odb.reader = input
    odb.define_schema do |s|
      s.date(:time)
      s.string(:label)
      s.float(:temp)
    end
    odb.run
    
    # now make sure the data is there
    rows = odb.conn.query("select * from #{series} where label = '#{rnd_str}'")
    # [{"name"=>"output_test", "tags"=>nil, "values"=>[{"time"=>"2015-01-10T23:00:50Z", "label"=>"474a550e0b2597ae71da9a6bbb2f9211", "temp"=>66.2}]}]
    expect(rows.count).to be(1)
    expected = [{"name"=>"output_test", "tags"=>nil, "values"=>[{"time"=>"2015-01-10T23:00:50Z", "label"=>rnd_str, "temp"=>66.2}]}]
    expect(rows).to eq(expected)
  end
end
