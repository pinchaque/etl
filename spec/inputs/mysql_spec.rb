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
require 'mysql2'

require 'etl/core'


RSpec.describe Job, :type => :input do
  it "mysql input each" do
    # add data to the test db
    dbconfig = Rails.configuration.database_configuration['test_mysql']
    client = Mysql2::Client.new(
      :host => dbconfig["host"],
      :database => dbconfig["database"],
      :username => dbconfig["username"],
      :password => dbconfig["password"],
      :flags => Mysql2::Client::MULTI_STATEMENTS)
    
    table_name = "test_table"
    
    insert_sql = <<SQL
drop table if exists #{table_name};
create table #{table_name} (day timestamp, attribute varchar(100));
insert into #{table_name} (day, attribute) values
  ('2015-04-01', 'rain'),
  ('2015-04-02', 'snow'),
  ('2015-04-03', 'sun');
SQL
    client.query(insert_sql)
    while client.next_result
      result = client.store_result
    end

    input = ETL::Input::MySQL.new(client, <<SQL)
select day, attribute
from test_table
order by day asc
SQL

    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq(Time.new(2015, 4, 1))
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq(Time.new(2015, 4, 2))
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq(Time.new(2015, 4, 3))
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)

    # test second iteration through the file
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)
   end
end
