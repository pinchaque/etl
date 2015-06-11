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

# Test loading into postgres
class TestPgCreate1 < ETL::Job::RelationalDB
  def initialize(conn)
    super(conn)
    @feed_name = "test_1"
    @input_file = "#{Rails.root}/spec/data/simple1.csv"
    @schema = ETL::Schema::Table.new(
      "day" => :date,
      "condition" => :string,
      "value_int" => :int,
      "value_num" => ETL::Schema::Type.new(:numeric, 10, 1),
      "value_float" => :float
    )
  end
end

RSpec.describe Job, :type => :job do

  it "postgres - insert" do
    dbconfig = Rails.configuration.database_configuration[Rails.env]
    conn = PGconn.open(
        :dbname => dbconfig["database"],
        :user => dbconfig["username"],
        :password => dbconfig["password"],
        :host => dbconfig["host"]
        )

    # Create destination table
    sql = <<SQL
drop table if exists test_1;
create table test_1 (
  day timestamp, 
  condition varchar, 
  value_int int, 
  value_num numeric(10, 1), 
  value_float float);
SQL
    conn.exec(sql)


    job = TestPgCreate1.new(conn)
    batch = ETL::Job::DateBatch.new(2015, 3, 31)

    jr = job.run(batch)

    expect(jr.status).to eq(:success)
    expect(jr.num_rows_success).to eq(3)
    expect(jr.num_rows_error).to eq(0)
  end
end
