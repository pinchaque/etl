require 'singleton'

module ETL::Job
  class Manager
    class Node
      attr_reader :id, :children
      def initialize(id, child = nil)
        @id = id
        @children = []
        add_child(child) unless child.nil?
      end
      
      def add_child(child)
        @children.push(child) unless @children.include? child
      end

      def ==(another_sock)
        self.id == another_sock.id
      end
    end

    include Singleton

    # Map of id => Class
    attr_accessor :job_classes, :job_class_factories

    def initialize
      @job_classes = {}
      @job_class_factories = {}
      @job_dependencies = {}
      @job_parents = []
    end

    # Registers a job class with the manager. This is typically called by
    # subclasses to register themselves with a convenient id to represent
    # that subclass. Only registered jobs can be executed.
    def register(id, klass, klass_factory=nil)
      id_str = id.to_s
      ETL.logger.debug("Registering job class with manager: #{id_str} => #{klass}")
      if @job_classes.has_key?(id)
        ETL.logger.warn("Overwriting previous registration of: #{id_str} => #{@job_classes[id_str]}")
      end

      if !klass_factory.nil?
        ETL.logger.debug("Registering job class factory with manager: #{id_str}")
        if @job_classes.has_key?(id_str)
          ETL.logger.warn("Overwriting previous registration of job factory: #{id_str}")
        end
        @job_class_factories[id_str] = klass_factory
      end

      @job_classes[id_str] = klass
    end

    # Registers a job class that depends on parent job with the manager
    def register_job_with_parent(id, p_id, klass, klass_factory=nil)
      register(id, klass, klass_factory)
      id_str = id.to_s
      pid_str = p_id.to_s

      ETL.logger.debug("Registering dependency with manager: #{id_str} depends on #{pid_str}")

      node = @job_dependencies.fetch(id_str, Node.new(id_str))
      if !@job_dependencies.include? pid_str
        pnode = Node.new(pid_str)
        pnode.add_child(node)
        @job_parents.push(pnode)
        @job_parents.delete(node)
      else
        pnode = @job_dependencies[pid_str]
        pnode.add_child(node)
      end

      # Build a hash to keep dependencies
      @job_dependencies[id_str] = node 
      @job_dependencies[pid_str] = pnode 
    end

    def sorted_dependent_jobs 
      output = [] 
      queue = @job_parents 
      visited = []

      while !queue.empty?
        node = queue.shift

        unless visited.include? node
          output.push(node.id)
          visited.push(node)
        end
        node.children.each { |c| queue.push(c) }
      end
      output
    end

    # Returns the job class registered for the specified ID, or nil if none
    def get_class(id)
      @job_classes[id.to_s]
    end

    # Returns the job class factory registered for the specified ID, or nil if none
    def get_class_factory(job_id)
      @job_class_factories[job_id.to_s]
    end

    # Iterates through the loaded classes specified during class initialization.
    def each_class(&block)
      @job_classes.each do |id, klass|
        yield klass
      end
    end
  end
end
