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


module ETL::Schema

  # Represents single data table including an ordered set of columns with
  # names and types.
  # columns: Hash of column name to ETL::Schema::Column objects
  # partition_columns: Hash of batch identifier to column name that is used
  #   for that partition; used for partition loads
  # primary_key: Array of columns that are primary keys; used for upsert
  #   and update loads
  class Table
    attr_accessor :columns, :partition_columns, :primary_key

    def initialize
      @columns = {}
      @partition_columns = {}
      @primary_key = []
    end
    
    def self.from_sequel_schema(schema)
      t = Table.new
      schema.each do |col|
        col_name = col[0]
        col_opts = col[1]
        
        # translate the database type from Sequel to our types
        type = case col_opts[:type]
        when :integer 
          :int
        when :datetime
          :date
        else
          col_opts[:type]
        end
        
        # TODO need to handle width and precision properly
        t.add_column(col_name, type, nil, nil)
      end
      return t
    end

    def to_s
      a = Array.new
      @columns.each do |k, v|
        a << "#{k.to_s} #{v.to_s}"
      end 
      "(\n  " + a.join(",\n  ") + "\n)\n"
    end

    def add_column(name, type, width, precision, &block)
      t = Column.new(type, width, precision)
      @columns[name.to_s] = t
      yield t if block_given?
    end

    def date(name, &block)
      add_column(name, :date, nil, nil, &block)
    end

    def string(name, &block)
      add_column(name, :string, nil, nil, &block)
    end

    def int(name, &block)
      add_column(name, :int, nil, nil, &block)
    end

    def float(name, &block)
      add_column(name, :float, nil, nil, &block)
    end

    def numeric(name, width, precision, &block)
      add_column(name, :numeric, width, precision, &block)
    end
  end
end
