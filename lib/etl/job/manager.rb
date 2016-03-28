module ETL::Job
  class Manager
    
    # function to load all job classes in the specified directory
    def self.load_job_classes(class_dir)
      unless class_dir.start_with?("/")
        class_dir = ETL.root + "/" + class_dir
      end
      ::Dir.new(class_dir).each do |file|
        next unless file =~ /\.rb$/
        path = class_dir + "/" + file
        ETL.logger.info("Loading user job class #{path}")
        require path
      end
    end
    
    # Instantiate with list of jobs that should be an array of unique class
    # names in string format
    def initialize(jobs)
      @jobs = jobs
    end
    
    def each_class(&block)
      @jobs.each do |klass|
        yield Object::const_get(klass.to_s)
      end
    end
  end
end
