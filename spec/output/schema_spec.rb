require 'etl/core'


RSpec.describe "jobs" do

  it "schema constructor" do

    s = ETL::Schema::Table.new
    s.date("day")
    s.string("condition")
    s.int("int")
    s.numeric("num1", 10, 1)
    s.numeric("num2", 10, 1)
    s.numeric("num3", nil, 2)
    s.float("float")
    
    # expected values based on above constructor
    exp_arr = [
      {name: "day", type: :date, width: nil, precision: nil},
      {name: "condition", type: :string, width: nil, precision: nil},
      {name: "int", type: :int, width: nil, precision: nil},
      {name: "num1", type: :numeric, width: 10, precision: 1},
      {name: "num2", type: :numeric, width: 10, precision: 1},
      {name: "num3", type: :numeric, width: nil, precision: 2},
      {name: "float", type: :float, width: nil, precision: nil},
    ]
    
    expect_schema(s, exp_arr)
  end

  it "schema construct from sequel" do
    
    ss = [
      [:id, {:allow_null => true, :db_type => 'integer', :primary_key => true, :type => :integer}],
      [:day, {:allow_null => true, :db_type => 'datetime', :primary_key => false, :type => :datetime}],
      [:condition, {:allow_null => true, :db_type => 'varchar(255)', :primary_key => false, :type => :string}],
      [:float, {:allow_null => true, :db_type => 'float', :primary_key => false, :type => :float}]
    ]

    s = ETL::Schema::Table.from_sequel_schema(ss)
    
    # expected values based on above constructor
    exp_arr = [
      {name: "id", type: :int, width: nil, precision: nil},
      {name: "day", type: :date, width: nil, precision: nil},
      {name: "condition", type: :string, width: nil, precision: nil},
      {name: "float", type: :float, width: nil, precision: nil},
    ]
    
    expect_schema(s, exp_arr)
  end
  
  def expect_schema(schema, exp_arr)
    c = schema.columns
    a = c.keys
    expect(a.length).to eq(exp_arr.length)

    for i in 0..(a.length - 1)
      exp = exp_arr[i]
      name = exp[:name]
      expect(a[i]).to eq(name)

      ct = c[name]

      expect(ct.type).to eq(exp[:type])

      if exp[:width].nil?
        expect(ct.width).to be_nil
      else
        expect(ct.width).to eq(exp[:width])
      end

      if exp[:precision].nil?
        expect(ct.precision).to be_nil
      else
        expect(ct.precision).to eq(exp[:precision])
      end

    end
  end

  it "print schema" do

    {
      [:string] => "string",
      [:string, 250] => "string(250)",
      [:string, 250, 10] => "string(250, 10)",
      [:numeric] => "numeric",
      [:numeric, 250] => "numeric(250)",
      [:numeric, nil, 10] => "numeric(0, 10)",
    }.each do |k, v|
      t = ETL::Schema::Column.new(k[0], k.at(1), k.at(2))
      expect(t.to_s()).to eq(v)
    end
    
    s = ETL::Schema::Table.new
    s.date("day")
    s.string("condition")
    s.int("int")
    s.numeric("num1", 10, 1)
    s.numeric("num2", 10, 1)
    s.numeric("num3", nil, 2)
    s.float("float")

    out = <<END
(
  day date,
  condition string,
  int int,
  num1 numeric(10, 1),
  num2 numeric(10, 1),
  num3 numeric(0, 2),
  float float
)
END

    expect(s.to_s()).to eq(out)
  end
end
