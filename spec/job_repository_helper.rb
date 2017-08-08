require 'pg'
require 'erb'
require 'singleton'
require 'etl/job/result'

class JobRunRepositoryHelper
  include Singleton

  def initialize()
    @conn_params = ETL.config.core[:database]
    @conn ||= PG.connect(@conn_params)
  end
  def delete_all
    puts "delete all"
    @conn.exec("DELETE from public.job_runs")
  end
  def setup
    @conn.exec("DROP SCHEMA IF EXISTS test CASCADE")
    @conn.exec("CREATE SCHEMA IF NOT EXISTS public")
    @conn.exec(ETL::Model::JobRunRepository.create_table_sql("public"))
  end
  def teardown
    @conn.exec("DROP TABLE IF EXISTS job_runs CASCADE")
  end
end

