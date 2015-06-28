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

RSpec.describe Job, :type => :transform do

  it "date trunc - day" do

    # array of inputs and expected outputs
    d = [
      ["2015-04-01", "2015-04-01"],
      ["2015-04-01 02:34:31", "2015-04-01"],
      ["2015-04-01Z11:34:31.23", "2015-04-01"],
      ["2015-04-10", "2015-04-10"],
      ["2015-05-11", "2015-05-11"],
      ["abcd", nil],
      ["", nil],
    ]

    t = ETL::Transform::DateTrunc("d").new
    d.each do |a|
      expect(a.length).to eq(2)
      if a[1].nil?
        expect(t.transform(a[0])).to be_nil
      else
        expect(t.transform(a[0])).to eq(a[1])
      end
    end
  end

  # TODO date trunc for hour, month, quarter, year
end
