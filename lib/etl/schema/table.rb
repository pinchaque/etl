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

  # Class representing type of a single column including width and precision
  class Type
    attr_accessor :type, :width, :precision

    def initialize(type, width = nil, precision = nil)
      @type = type.to_sym()
      @width = width
      @precision = precision
    end

    def to_s
      s = type.to_s()
      if not width.nil? or not precision.nil?
        s += "("
        s += width.nil? ? "0" : width.to_s()
        if not precision.nil?
          s += ", #{precision}"
        end
        s += ")"
      end
      return s
    end
  end


  # Represents single data table including an ordered set of columns with
  # names and types.
  class Table
    attr_accessor :columns

    # Initialize schema based on passed in columns. Argument is a hash with
    # the keys being the column names and the values being one of the following:
    #   * ETL::Schema::Type - the type object to use
    #   * symbol - the type of this column (width and precision are not set)
    #   * hash - hash containing :type, :width (opt), :precision (opt)
    #   * array - contains [type, width=nil, precision=nil]
    def initialize(columns)
      @columns = Hash.new
      columns.each do |k, v|
        case v.class.name
        when "ETL::Schema::Type" then
          v2 = v.clone()
        when "Array" then
          v2 = Type.new(v[0], v.at(1), v.at(2))
        when "Symbol" then
          v2 = Type.new(v)
        when "Hash" then
          v2 = Type.new(v[:type], 
            v.fetch(:width, nil), 
            v.fetch(:precision, nil))
        else
          raise "Invalid type #{v.class.name}"
        end

        @columns[k] = v2
      end
    end

    def to_s
      a = Array.new
      @columns.each do |k, v|
        a << "#{k.to_s} #{v.to_s}"
      end 
      "(\n  " + a.join(",\n  ") + "\n)\n"
    end
  end
end
