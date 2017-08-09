require 'etl/models/job_run_repository'
require_relative '../command'

module ETL::Cli::Cmd
  class Schema < ETL::Cli::Command
    # Shared functions across all schema commands
    class Base < ETL::Cli::Command
      def drop_tables()
        jrr = ::ETL::Model::JobRunRepository.instance
        jrr.tables.each do |t|
          log.info("  * Dropping #{t}")
          puts t.inspect
          jrr.conn.exec("DROP TABLE IF EXISTS #{t} CASCADE")
        end
      end
      def show_tables(details = false)
        jrr = ::ETL::Model::JobRunRepository.instance
        jrr.tables.each do |t|
          log.info("  * #{t}")
          if details
            jrr.table_schema(t).each do |key, value|
              name = key
              typ = value
              log.info("    - #{"%-20s" % name} #{typ}")
            end
          end
        end
      end
    end

    class Print < Base
      option ['-f', '--force'], :flag, 'Force deletion and recreation of existing tables'
      def execute
        show_tables(true)
      end
    end
    class Create < Base
      option ['-f', '--force'], :flag, 'Force deletion and recreation of existing tables'
      def execute
        jrr = ::ETL::Model::JobRunRepository.instance
        log.info("Initializing schema")
        # Handle case when tables already exist
        tables = jrr.tables
        if tables.length > 0
          if force?
            log.info("Forcing removal of existing tables...")
            drop_tables()
          else
            log.info("This script is cowardly refusing to proceed because the following tables exist:")
            show_tables()
            log.info("You can also re-run with the --force option to force delete and recreate.")
            exit(0)
          end
        end

        log.info("Creating schema...")

        jrr.create_table
        log.info("Done! The following tables have been created:")
        show_tables()
      end
    end
    class Destroy < Base
      option ['-y', '--yes'], :flag, 'Really proceed with deletion'
      def execute
        if @yes
          log.info("Dropping tables")
          drop_tables()
        else
          log.info("The following tables will be dropped. Re-run with -y to proceed.")
          show_tables()
        end
      end
    end
    subcommand 'create', 'Creates the ETL system jobs schema', Schema::Create
    subcommand 'destroy', 'Destroys the ETL system jobs schema', Schema::Destroy
    subcommand 'print', 'Prints existing schema details', Schema::Print
  end
end
