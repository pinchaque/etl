
module ETL::Job
  class Manager
    def initialize(params = {})
      @jobs = params.dup
    end
    
    def each_class(&block)
      @jobs.each do |id, h|
        yield Object::const_get(h[:class] || id)
      end
    end
  end
end
