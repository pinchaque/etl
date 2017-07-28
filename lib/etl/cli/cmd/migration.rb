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

      def schema_map
        @schema_map ||= begin
          schema_hash = source_schema.each_with_object({}) do |schema, h|
            column_name = columns[schema[0].to_sym]
            h[column_name] = schema[1][:type] 
          end
          schema_hash.select { |k, v| columns.values.include? k } .sort_by { |k, _| columns.values.index(k) }.to_h
        end
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

      def up_sql
        t = ETL::Redshift::Table.new(table)

        schema_map.each do |key, type|
          case type
          when :int
            t.int(key.to_sym)
          when :float
            t.float(key.to_sym)
          when :double
            t.double(key.to_sym)
          when :string
            t.string(key.to_sym)
          when :character
            t.character(key.to_sym)
          when :boolean
            t.boolean(key.to_sym)
          when :text
            t.text(key.to_sym)
          when :datetime
            t.date(key.to_sym)
          else
            if type.to_s.start_with? "varchar"
              range = type.to_s.split("(")[1].split(")")[0]
              t.varchar(key.to_sym, range.to_i)
            end

            t.int(key.to_sym) if type.to_s.start_with? "int"
            t.smallint(key.to_sym) if type.to_s.start_with? "tinyint"
          end
        end

        primary_keys.each { |pk| t.add_primarykey(pk) }
        up = <<END
        @client.execute("#{t.create_table_sql}")
END
      end

      def down_sql
        down = <<END
        @client.execute("drop table #{@table}")
END
      end

      def execute
        create_migration(up_sql, down_sql)
      end
    end

    subcommand 'create', 'Create migration', Migration::Create
  end
end
