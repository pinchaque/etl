require 'etl/transform/base.rb'

module ETL::Transform

  class Zip5 < Base

    # Validates that the specified value is a well-formatted 5-digit zip code
    def initialize
      super()
    end

    # Truncates the date
    def transform(value)
      return nil if value.nil?

      v = value.dup

      # clean out leading/trailing whitespace
      v.strip!

      # We expect the string to just be digits and hyphen, otherwise this
      # isn't a zip code
      return nil unless v =~ /^\d+(-\d*)?+$/

      # Chop everything off from hyphen onwards
      v.gsub!(/-.*$/, "")

      # Pad left with "0" if needed
      v = v.rjust(5, "0")

      # Return first 5 chars
      v[0..4]
    end
  end
end
