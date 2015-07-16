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


require 'etl/core'

RSpec.describe "transforms" do

  it "zip codes" do

    # array of inputs and expected outputs
    d = [
      ["92101", "92101"],
      ["  92101", "92101"],
      ["92101  ", "92101"],
      ["501", "00501"],
      ["50", "00050"],
      ["92101-3437", "92101"],
      ["92101-34378585", "92101"],
      ["92101-34378585", "92101"],
      ["921013437", "92101"],
      ["  921013437", "92101"],
      ["abcd", nil],
      ["92101xyz", nil],
      ["xx92101 ", nil],
      ["", nil],
    ]

    t = ETL::Transform::Zip5.new
    d.each do |a|
      expect(a.length).to eq(2)
      if a[1].nil?
        expect(t.transform(a[0])).to be_nil
      else
        expect(t.transform(a[0])).to eq(a[1])
      end
    end
  end
end
