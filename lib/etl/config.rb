require 'etl/util/hash_util'
require 'psych'
require 'singleton'

module ETL
  
  # Configuration class
  class Config
    attr_accessor :config_dir, :db_file, :db, :schema_file, :schema, :etl_file, :etl
    
    include Singleton
    
    def initialize
      @config_dir = File.expand_path('../../../etc', __FILE__)
    end
    
    def db_file
      @db_file ||= @config_dir + "/database.yml"
    end
    
    def db
      @db ||= load_config(db_file)
    end
      
    def schema_file
      @schema_file ||= @config_dir + "/schema.yml"
    end
    
    def schema
      @schema ||= load_config(schema_file)
    end
    
    def core_file
      @core_file ||= @config_dir + "/core.yml"
    end
    
    def core
      @core ||= load_config(core_file)
    end
    
    def load_config(file)
      ETL::HashUtil::symbolize_keys(Psych.load_file(file))
    end
  end
  
  def self.config
    Config.instance
  end
end
