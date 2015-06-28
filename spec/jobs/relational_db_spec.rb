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


class TestPgLoad1 < ETL::Job::PostgreSQL
  def initialize(input, conn, table_name)
    super(input, conn)
    @feed_name = table_name
    define_schema do |s|
      s.date(:day)
    end
  end
end


RSpec.describe Job, :type => :job do


  # test out our formatting of values
  it "value formatting" do
    input = ETL::Input::Array.new([])
    job = TestPgLoad1.new(input, nil, "xxx")

    d = [
      {type: :int, value: 1, expected: "1"},
      {type: :float, value: 1.2, expected: "1.2"},
      {type: :numeric, value: 1.3, expected: "1.3"},
      {type: :string, value: "hello", expected: "'hello'"},
      {type: :blah, value: "hello", expected: "'hello'"},

      {type: :int, value: nil, expected: "null"},
      {type: :float, value: nil, expected: "null"},
      {type: :numeric, value: nil, expected: "null"},
      {type: :string, value: nil, expected: "null"},
      {type: :blah, value: nil, expected: "null"},
    ]

    d.each do |h|
      col = ETL::Schema::Column.new(h[:type])
      actual = job.value_to_db_str(col, h[:value])
      expect(actual).to eq(h[:expected])
    end
  end
end
