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

  it "base job id" do

    b = ETL::Job::Base.new
    
    d = [
      [{ :day => "2015-04-01" }, "20150401"],
      [{ :day => "New York" }, "newyork"],
      [{ :day => "Random Identifier #123  " }, "randomidentifier123"],
      [{ :day => "2015-04-01", :city => "New York" }, "newyork_20150401"],
      [{ :city => "New York", :day => "2015-04-01" }, "newyork_20150401"],
      [{ :color => "cyan", :city => "New York", :day => "2015-04-01" }, "newyork_cyan_20150401"],
    ]
    
    d.each do |a|
      expect(a.length).to eq(2)
      b.batch = a[0]
      expect(b.batch_id).to eq(a[1])
    end
  end
end
