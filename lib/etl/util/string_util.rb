module ETL::StringUtil
  # Strips off everything but the base class name
  def self.base_class_name(class_name)
    return nil if class_name.nil?
    class_name.to_s.gsub(/^.*::/, '')
  end
  
  # Transforms string from CamelCase to snake_case
  def self.camel_to_snake(str)
    return nil if str.nil?
    word = str.dup
    word.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
    word.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
    word.tr!("-", "_")
    word.downcase!
    word
  end

  # Transforms string from snake_case to CamelCase
  def self.snake_to_camel(str)
    return nil if str.nil?
    str.capitalize.gsub(/_(\w)/){$1.upcase}
  end

  # Transforms integer to n-digit string 
  def self.digit_str(i, n=4)
    i.to_s.rjust(n, "0")
  end
end
