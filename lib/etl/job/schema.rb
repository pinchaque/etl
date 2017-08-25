# Class for creating and destroying the schema needed to manage the jobs
module ETL::Job
  class Schema
    
    def initialize(con)
      @con = con
    end
    
    def exist?
      @con.tables.length > 0
    end
    
    def create
      raise "Tables already exist" if exist?
      @con.create_table(:job_runs) do
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
    end
    
    def destroy
      @con.tables.each do |t|
        @con.drop_table(t)
      end
    end
  end
end
