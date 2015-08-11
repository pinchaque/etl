module ETL::Transform
  class Base
    def initialize
    end

    # Do nothing
    def transform(value)
      value
    end
  end
end
