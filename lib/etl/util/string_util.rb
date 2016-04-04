module ETL::StringUtil
  # Strips off everything but the base class name
  def self.base_class_name(class_name)
    class_name.to_s.gsub(/^.*::/, '')
  end
  
  # Transforms string from CamelCase to snake_case
  def self.camel_to_snake(str)
    word = str.dup
    word.gsub!(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
    word.gsub!(/([a-z\d])([A-Z])/,'\1_\2')
    word.tr!("-", "_")
    word.downcase!
    word
  end
end
