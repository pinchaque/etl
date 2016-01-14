module ETL::HashUtil
  # Turns all keys in specified hash into their symbol form by calling to_sym
  def self.symbolize_keys(value)
    return value unless value.is_a?(Hash)
    value.inject({}){|x,(k,v)| x[k.to_sym] = symbolize_keys(v); x}
  end
end
