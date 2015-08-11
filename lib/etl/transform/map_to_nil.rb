require 'etl/transform/base.rb'

module ETL::Transform

  # Maps the specified values to nil, which is useful for input sources
  # such as CSV files that don't natively support nil.
  class MapToNil < Base

    def initialize(*values)
      super()
      @values = {}
      values.to_a.each do |v|
        @values[v] = 1
      end
    end

    def transform(value)
      @values.has_key?(value) ? nil : value
    end
  end
end
