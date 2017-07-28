module ETL::Job

  # Base class for all migratable jobs that are run
  class MigratableBase < Base

    def migration_files
      Dir["#{migration_dir}/#{id}_*.rb"]
    end

    def migration_dir
      Dir.pwd 
    end

    def target_version
      1
    end

    def set_schema_version(version)
      ENV["#{env_name}"] = version.to_s 
    end

    def get_schema_version
      ENV["#{env_name}"]
    end

    def env_name
      "#{id.upcase}_SCHEMA_VERSION"
    end
      
    def deploy_version
      @deploy_version ||= begin
        version = get_schema_version
        raise "#{env_name} is not set" unless version
        version.to_i
      end
    end

    def migrate
      # execute migration
      return if deploy_version == target_version
      # To-do: execute 'down' when the target version is smaller than deploy version 
      # To-do: execute 'up' when the target version is greater than deploy version 
      if deploy_version < target_version
        start_version = deploy_version + 1
        goal_version = target_version
        move = 1 
      else
        start_version = deploy_version
        goal_version = target_version + 1 
        move = -1 
      end

      # Raise error message if the target version migration doesnt exist
      raise "Migration for version #{goal_version} does not exist in #{migration_dir}" unless migration_files.include? "#{migration_dir}/#{id}_#{ETL::StringUtil.digit_str(goal_version)}.rb"
       
      current_version = start_version
      while true
        version = ETL::StringUtil.digit_str(current_version) 
        file = "#{id}_#{version}"
        load "#{migration_dir}/#{file}.rb"
        clazz = "Migration::#{ETL::StringUtil::snake_to_camel(file)}".split('::').inject(Object) {|o,c| o.const_get c}
        m = clazz.new
        if move == 1 
          m.up
        else
          m.down
        end
        break if current_version == goal_version
        current_version += move 
      end
      set_schema_version(target_version)
    end

    def run
      migrate
      super
    end
  end
end
