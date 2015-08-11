
module ETL::Schema

  # Class representing a single column including width and precision
  class Column
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
end
