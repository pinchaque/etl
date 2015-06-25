###############################################################################
# Copyright (C) 2015 Chuck Smith
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
###############################################################################

require 'rails_helper'
require 'etl/core'


RSpec.describe Job, :type => :job do

  it "schema constructor" do

    s = ETL::Schema::Table.new
    s.date("day")
    s.string("condition") do |col|
      col.input_field("attribute")
    end
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

    c = s.columns
    a = c.keys
    expect(a.length).to eq(exp_arr.length)

    for i in 0..(a.length - 1)
      exp = exp_arr[i]
      name = exp[:name]
      expect(a[i]).to eq(name)

      ct = s.columns[name]

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
    s.string("condition") do |col|
      col.input_field("attribute")
    end
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
