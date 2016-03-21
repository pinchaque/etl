#!/usr/bin/env ruby
require 'optparse'

require File.expand_path("../../lib", __FILE__) + "/etl"

def date_puts(msg)
  puts("[#{DateTime.now.strftime("%F %T")}] #{msg}")
end

options = {
  :database => "development"
}
OptionParser.new do |opts|
  opts.banner = "Usage: jobs_schema_init.rb [options]"
  opts.on("-f", "--force", "Force deletion and recreation of existing tables") do |o|
    options[:force] = true
  end
  opts.on("-d", "--database NAME", "Specify the database identifier to use (from database.yml)") do |o|
    options[:database] = o
  end
end.parse!

date_puts("Initializing schema for database id '#{options[:database]}'")

conn = Sequel.connect(ETL.config.core[:database])

# Handle case when tables already exist
tables = conn.tables
if tables.length > 0
  if options[:force]
    date_puts("Forcing removal of existing tables...")
    tables.each do |t|
      date_puts("  * Dropping #{t}") 
      conn.drop_table(t)
    end
  else
    date_puts(<<MSG)
This script is cowardly refusing to proceed because the following tables exist:
#{tables.map{|t| "  * #{t}"}.join("\n")}
You can also re-run with the "--force" option to force delete and recreate.
MSG
    exit(0)
  end
end

date_puts("Creating initial schema...")

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
  Integer :num_rows_success
  Integer :num_rows_error
  String :message
end

date_puts(<<MSG)
Done! The following tables have been created:
#{conn.tables.map{ |t| "  * #{t}"}.join("\n")}
MSG
