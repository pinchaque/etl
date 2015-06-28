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

RSpec.describe Job, :type => :input do

  it "input transforms" do
    data = [
      { "day" => "2015-04-01 10:03:04", "id" => "-1", "zip_code" => "501" },
      { "day" => "2015-04-02 23:45:01", "id" => "1", "zip_code" => "92101" },
      { "day" => "2015-04-03", "id" => "2", "zip_code" => "98101-3437" },
      { "day" => "2015-04-04", "id" => "3", "zip_code" => "" },
    ]

    input = ETL::Input::Array.new(data)

    input.pre_transform = ETL::Transform::MapToNil.new("")

    # We can use lambda functions to transform
    # or chain together transform classes
    input.transforms = {
      day: ETL::Transform::DateTrunc.new("d"),
      value: ETL::Transform::Zip5.new,
    }

    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['id']).to be_nil
        expect(row['zip_code']).to eq('00501')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['id']).to eq(1)
        expect(row['zip_code']).to eq('92101')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['id']).to eq(2)
        expect(row['zip_code']).to eq('98101')
      when 2 then
        expect(row['day']).to eq('2015-04-04')
        expect(row['id']).to eq(3)
        expect(row['zip_code']).to be_nil
      else
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end
end
