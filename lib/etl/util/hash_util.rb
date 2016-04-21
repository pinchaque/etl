module ETL::HashUtil
  # Turns all keys in specified hash into their symbol form by calling to_sym
  def self.symbolize_keys(value)
    return value unless value.is_a?(Hash)
    value.inject({}){|x,(k,v)| x[k.to_sym] = symbolize_keys(v); x}
  end
    
  # Turns all keys in specified hash into their string form by calling to_s
  def self.stringify_keys(value)
    return value unless value.is_a?(Hash)
    value.inject({}){|x,(k,v)| x[k.to_s] = stringify_keys(v); x}
  end
  
  def self.sanitize(h_orig, replacement = "")
    return h_orig unless h_orig.is_a?(Hash)
    h = h_orig.dup
    h.each do |k, v|
      h[k] = %w(passwd password).include?(k.to_s) ? replacement : sanitize(v, replacement)
    end
    h
  end
end
