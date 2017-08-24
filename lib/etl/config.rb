require 'etl/util/hash_util'
require 'psych'
require 'singleton'

module ETL
  
  # Configuration class
  class Config
    attr_accessor :config_dir, :db 
    
    include Singleton
    
    def initialize
      @config_dir = File.expand_path('../../../etc', __FILE__)
    end
    
    def db_file
      @config_dir + "/database.yml"
    end
    
    def db(&b)
      @db ||= self.class.load_file(db_file)
      yield @db if block_given?
      @db
    end
    
    def core_file
      @config_dir + "/core.yml"
    end
    
    def core(&b)
      c = self.class.load_file(core_file)
      yield c if block_given?
      c
    end
    
    def self.load_file(file)
      ETL::HashUtil::symbolize_keys(Psych.load_file(file))
    end
  end
  
  def self.config
    Config.instance
  end
  
  def ETL.configure
    yield ETL.config
  end
end
