require 'sequel'
require 'json'

module ETL::Model
  class Job < ::Sequel::Model

    def output_params_hash
      params_parse(self.output_params)
    end
    
    def output_params_hash=(h)
      self.output_params = params_set(h)
    end
    
    def input_params_hash
      params_parse(self.input_params)
    end
    
    def input_params_hash=(h)
      self.input_params = params_set(h)
    end
    
    private
    
    def params_parse(p)
      return nil if p.nil?
      ETL::HashUtil::symbolize_keys(JSON.parse(p))
    end
    
    def params_set(h)
      h.nil? ? nil : h.to_json
    end
  end
  
  Job.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
