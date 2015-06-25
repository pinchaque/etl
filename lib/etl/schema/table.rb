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
  class Table
    attr_accessor :columns

    def initialize
      @columns = Hash.new
    end

    def to_s
      a = Array.new
      @columns.each do |k, v|
        a << "#{k.to_s} #{v.to_s}"
      end 
      "(\n  " + a.join(",\n  ") + "\n)\n"
    end

    def add_column(name, type, width, precision, input_name, &block)
      t = Column.new(type, width, precision)
      t.input_name = input_name
      @columns[name] = t
      yield t if block_given?
    end

    def date(name, input_name = nil, &block)
      add_column(name, :date, nil, nil, input_name, &block)
    end

    def string(name, input_name = nil, &block)
      add_column(name, :string, nil, nil, input_name, &block)
    end

    def int(name, input_name = nil, &block)
      add_column(name, :int, nil, nil, input_name, &block)
    end

    def float(name, input_name = nil, &block)
      add_column(name, :float, nil, nil, input_name, &block)
    end

    def numeric(name, width, precision, input_name = nil, &block)
      add_column(name, :numeric, width, precision, input_name, &block)
    end
   end
end
