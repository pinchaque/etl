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

  it "date trunc" do

    # array of inputs and expected outputs
    d = {
      "2015-04-01" => 
      {
        minute: "2015-04-01 00:00:00",
        hour: "2015-04-01 00:00:00",
        day: "2015-04-01 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },
      
      "2015-04-03 02:34:31" =>
      {
        minute: "2015-04-03 02:34:00",
        hour: "2015-04-03 02:00:00",
        day: "2015-04-03 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      "2015-04-03 11:34:31.23" =>
      {
        minute: "2015-04-03 11:34:00",
        hour: "2015-04-03 11:00:00",
        day: "2015-04-03 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      "2015-05-11" =>
      {
        minute: "2015-05-11 00:00:00",
        hour: "2015-05-11 00:00:00",
        day: "2015-05-11 00:00:00",
        week: "2015-05-10 00:00:00",
        month: "2015-05-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      # test to make sure week rounding works across years
      "2014-12-27 23:59:59" => { week: "2014-12-21 00:00:00" },
      "2014-12-28 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-29 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-30 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-31 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-01 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-02 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-03 23:59:59" => { week: "2014-12-28 00:00:00" },
      "2015-01-04 00:00:00" => { week: "2015-01-04 00:00:00" },

      "abcd" =>
      {
        minute: nil,
        hour: nil,
        day: nil,
        week: nil,
        month: nil,
        quarter: nil,
        year: nil,
      },

      "" =>
      {
        minute: nil,
        hour: nil,
        day: nil,
        week: nil,
        month: nil,
        quarter: nil,
        year: nil,
      },

      DateTime.parse("2015-05-11 12:34:55") =>
      {
        minute: "2015-05-11 12:34:00",
        hour: "2015-05-11 12:00:00",
        day: "2015-05-11 00:00:00",
        week: "2015-05-10 00:00:00",
        month: "2015-05-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },
    }

    d.each do |input, tests|
      #puts("*** #{input} ***")
      tests.each do |resolution, expected|
        #puts("--- #{resolution} => #{expected} ---")
        t = ETL::Transform::DateTrunc.new(resolution)
        if expected.nil?
          expect(t.transform(input)).to be_nil
        else
          expect(t.transform(input)).to eq(expected)
        end
      end
    end
  end
end
