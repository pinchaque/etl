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

  it "array input each" do
    data = [
      { "col1" => "value1a", "col2" => "value2a", "col3" => "value3a" },
      { "col1" => "value1b", "col2" => "value2b", "col3" => "value3b" },
      { "col1" => "value1c", "col2" => "value2c", "col3" => "value3c" },
    ]
    input = ETL::Input::Array.new(data)

    # first time through the inputs
    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['col1']).to eq('value1a')
        expect(row['col2']).to eq('value2a')
        expect(row['col3']).to eq('value3a')
      when 1 then
        expect(row['col1']).to eq('value1b')
        expect(row['col2']).to eq('value2b')
        expect(row['col3']).to eq('value3b')
      when 2 then
        expect(row['col1']).to eq('value1c')
        expect(row['col2']).to eq('value2c')
        expect(row['col3']).to eq('value3c')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)

    # second time through the inputs
    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['col1']).to eq('value1a')
        expect(row['col2']).to eq('value2a')
        expect(row['col3']).to eq('value3a')
      when 1 then
        expect(row['col1']).to eq('value1b')
        expect(row['col2']).to eq('value2b')
        expect(row['col3']).to eq('value3b')
      when 2 then
        expect(row['col1']).to eq('value1c')
        expect(row['col2']).to eq('value2c')
        expect(row['col3']).to eq('value3c')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)
  end
end
