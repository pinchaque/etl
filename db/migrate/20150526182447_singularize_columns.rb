class SingularizeColumns < ActiveRecord::Migration
  def change
    change_table :job_runs do |t|
      t.rename :jobs_id, :job_id
      t.rename :job_statuses_id, :job_run_status_id
    end

    add_foreign_key :job_runs, :jobs
    add_foreign_key :job_runs, :job_run_statuses
  end
end
