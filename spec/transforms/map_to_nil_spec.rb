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

  it "map to nil" do
    t = ETL::Transform::MapToNil.new("", "[NULL]", -1)
    expect(t.transform("")).to be_nil
    expect(t.transform(-1)).to be_nil
    expect(t.transform("[NULL]")).to be_nil
    expect(t.transform("[null]")).to eq("[null]")
    expect(t.transform(1)).to eq(1)
  end
end
