require 'rails_helper'
require 'etl/core'


RSpec.describe Job, :type => :job do

  it "schema constructor" do
    
    schema = ETL::Schema::Table.new({
      "day" => :date,
      "condition" => :string,
      "int" => :int,
      "num1" => ETL::Schema::Type.new(:numeric, 10, 1),
      "num2" => [:numeric, 10, 1],
      "num3" => { type: :numeric, precision: 2 },
      "float" => :float
    })

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

    c = schema.columns
    a = c.keys
    expect(a.length).to eq(exp_arr.length)

    for i in 0..(a.length - 1)
      exp = exp_arr[i]
      name = exp[:name]
      expect(a[i]).to eq(name)

      ct = schema.columns[name]

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
      t = ETL::Schema::Type.new(k[0], k.at(1), k.at(2))
      expect(t.to_s()).to eq(v)
    end
    
    schema = ETL::Schema::Table.new({
      "day" => :date,
      "condition" => :string,
      "int" => :int,
      "num1" => ETL::Schema::Type.new(:numeric, 10, 1),
      "num2" => [:numeric, 10, 1],
      "num3" => { type: :numeric, precision: 2 },
      "float" => :float
    })

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

    expect(schema.to_s()).to eq(out)
  end
end
