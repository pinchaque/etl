require_relative '../command'

module ETL::Cli::Cmd
  class Schema < ETL::Cli::Command
    
    # Shared functions across all schema commands
    class Base < ETL::Cli::Command
      def connection(&block)
        Sequel.connect(ETL.config.core[:database]) do |conn|
          yield conn
        end
      end
      
      def drop_tables(conn)
        conn.tables.each do |t|
          log.info("  * Dropping #{t}") 
          conn.drop_table(t)
        end
      end
      
      def show_tables(conn, details = false)
        conn.tables.each do |t|
          log.info("  * #{t}")
          if details
            conn.schema(t).sort.each do |x|
              name = x[0]
              h = x[1]
              log.info("    - #{"%-20s" % name} #{h[:type]}")
            end
          end
        end
      end
    end
    
    class Print < Base
      option ['-f', '--force'], :flag, 'Force deletion and recreation of existing tables'
      def execute
        connection do |conn|
          show_tables(conn, true)
        end
      end
    end
    
    class Create < Base
      option ['-f', '--force'], :flag, 'Force deletion and recreation of existing tables'
      def execute
        log.info("Initializing schema")
        connection do |conn|
          # Handle case when tables already exist
          tables = conn.tables
          if tables.length > 0
            if force?
              log.info("Forcing removal of existing tables...")
              drop_tables(conn)
            else
              log.info("This script is cowardly refusing to proceed because the following tables exist:")
              show_tables(conn)
              log.info("You can also re-run with the --force option to force delete and recreate.")
              exit(0)
            end
          end
      
          log.info("Creating schema...")
          
          conn.create_table(:job_runs) do
            primary_key :id
            DateTime :created_at, null: false
            DateTime :updated_at, null: false
            String :job_id, :null => false, :index => true
            String :batch, :null => false, :index => true
            String :status, :null => false, :index => true
            DateTime :queued_at
            DateTime :started_at
            DateTime :ended_at
            Integer :rows_processed
            String :message
          end
      
          log.info("Done! The following tables have been created:")
          show_tables(conn)
        end
      end
    end
    
    class Destroy < Base
      option ['-y', '--yes'], :flag, 'Really proceed with deletion'
      def execute
        connection do |conn|
          if @yes
            log.info("Dropping tables")
            drop_tables(conn)
          else
            log.info("The following tables will be dropped. Re-run with -y to proceed.")
            show_tables(conn)
          end
        end
      end
    end
    
    subcommand 'create', 'Creates the ETL system jobs schema', Schema::Create
    subcommand 'destroy', 'Destroys the ETL system jobs schema', Schema::Destroy
    subcommand 'print', 'Prints existing schema details', Schema::Print
  end
end
