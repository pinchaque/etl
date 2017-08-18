require_relative '../command'
require 'etl/job/exec'
require 'sequel'
require 'erb'
require 'etl/redshift/table'

module ETL::Cli::Cmd
  class Migration < ETL::Cli::Command

    class Create < ETL::Cli::Command

      option "--table", "TABLE", "table name", :required => true
      option ['-p', '--provider'], "Provider", attribute_name: :provider
      option "--host", "Host", attribute_name: :host
      option "--user", "User", attribute_name: :user
      option "--password", "Password", attribute_name: :password
      option "--database", "Database", attribute_name: :database
      option "--inputdir", "Input directory that contains a configuration file", :attribute_name => :inputdir
      option "--outputdir", "Output directory where migration is created at", :attribute_name => :outputdir, :required => true 

      Adopter = { mysql: "mysql2" }

      class Generator
        attr_accessor :table, :version, :up, :down
        def template_binding
          binding
        end
      end

      def table_config
        @table_config ||= begin
          config_file = @inputdir + "/migration_config.yml"
          raise "Could not find migration_config.yml file under #{@inputdir}" unless File.file?(config_file)
          config_values = ETL::HashUtil.symbolize_keys(Psych.load_file(config_file))
          raise "#{table} is not defined in the config file" unless config_values.include? table.to_sym
          config_values[table.to_sym]
        end
      end

      def provider_params
        @provider_params ||= begin
          if @provider && @host && @user && @password && @database
            adapter = @provider
            adapter = Adopter[@provider] if Adopter.include? @provider
            return { host: host, adapter: adapter, database: database, user: user, password: password } 
          else
            raise "source_db_params is not defined in the config file" unless table_config.include? :source_db_params
            return table_config[:source_db_params]
          end  
          raise "Parameters to connect to the data source are required"
        end
      end

      def columns
        @columns ||= begin
          raise "columns are not defined in the config file" unless table_config.include? :columns 
          table_config[:columns]
        end
      end

      def scd_columns
        @scd_columns ||= begin
          raise "scd_columns are not defined in the config file" unless table_config.include? :scd_columns 
          table_config[:scd_columns]
        end 
      end

      def target_columns
        @target_columns ||= table_config.fetch(:target_columns, {})
      end

      def scd? 
        table_config.fetch(:scd, false)
      end

      def scd_table
        table + "_history"
      end

      def provider_connect
        @provider_connect ||= ::Sequel.connect(provider_params)
      end

      def primary_keys
        @primary_keys ||= begin
          source_schema.select { |column, types| types[:primary_key] == true }
                          .map{ |column, types| column }          
        end
      end

      def source_schema
        @source_schema ||= provider_connect.schema(@table)
      end

      def schema_map(scd = false)
        clms = if scd
                 scd_columns
               else
                 columns
               end

        schema_hash = source_schema.each_with_object({}) do |schema, h|
          column_name = clms[schema[0].to_sym]
          h[column_name] = schema[1][:db_type] 
        end
        s_map = schema_hash.select { |k, v| clms.values.include? k } .sort_by { |k, _| clms.values.index(k) }.to_h
        # Add target-specific columns if defined
        target_columns.each { |k, v| s_map[k] = v }
        s_map
      end

      def migration_version
        @migration_version ||= Dir["#{@outputdir}/#{table}_*.rb"].length
      end

      def create_migration(up, down)
        generator = Generator.new
        version = ETL::StringUtil.digit_str(migration_version+1)
        migration_file = File.open("#{@outputdir}/#{table}_#{version}.rb", "w")
        template = File.read("#{@inputdir}/redshift_migration.erb")
        generator.up = up
        generator.down = down 
        generator.table = table.capitalize 
        generator.version = version 
        migration_file << ERB.new(template).result(generator.template_binding)
        migration_file.close
      end

      def up_sql(scd = false)
        t = define_table(table, schema_map, primary_keys)
        up = <<END
        @client.execute('#{t.create_table_sql}')
END
        return up unless scd

        t_scd = define_table(scd_table, schema_map(true), primary_keys)
        up_scd = <<END
        @client.execute('#{t_scd.create_table_sql}')
END
        up + up_scd
      end

      def down_sql(scd = false)
        down = <<END
        @client.execute('drop table #{@table}')
END
        return down unless scd

        down_scd = <<END
        @client.execute('drop table #{@table}_history')
END
        down + down_scd
      end

      def define_table(table_name, schema, pks = [])
        t = ETL::Redshift::Table.new(table_name)

        # Create auto-increment key if scd
        if scd?
          auto_key = "#{table_name}_id".to_sym
          t.int(auto_key)
          temp_hash = {}
          temp_hash[auto_key] = { key: "identity" } 
          temp_hash.merge!(schema)
          schema.replace temp_hash
        end

        schema.each do |key, type|
          if type.is_a? Hash 
            define_type(t, key, type[:type])
            if type.include? :key
              case type[:key].to_sym
              when :identity
                t.set_identity(key)
              when :primary
                t.add_primarykey(key)
              end
            end
          else
            define_type(t, key, type.to_sym)
          end
        end
        pks.each { |pk| t.add_primarykey(pk) }
        t
      end

      def define_type(table, key, type)
        case type
        when :int
          table.int(key.to_sym)
        when :float
          table.float(key.to_sym)
        when :double
          table.double(key.to_sym)
        when :string
          table.string(key.to_sym)
        when :character
          table.character(key.to_sym)
        when :boolean
          table.boolean(key.to_sym)
        when :text
          table.varchar(key.to_sym, "max")
        when :datetime
          table.date(key.to_sym)
        else
          if type.to_s.start_with? "varchar"
            if type.to_s.include? "(" and type.to_s.include? ")"
              range = type.to_s.split("(")[1].split(")")[0]
              table.varchar(key.to_sym, range)
            else
              table.varchar(key.to_sym, "max")
            end
          end

          table.int(key.to_sym) if type.to_s.start_with? "int"
          table.boolean(key.to_sym) if type.to_s.start_with? "tinyint"
        end
      end

      def execute
        if scd?
          create_migration(up_sql(true), down_sql(true))
        else
          create_migration(up_sql, down_sql)
        end
      end
    end

    subcommand 'create', 'Create migration', Migration::Create
  end
end
