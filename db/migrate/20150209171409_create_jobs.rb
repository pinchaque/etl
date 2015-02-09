class CreateJobs < ActiveRecord::Migration
  def change
    create_table :jobs do |t|
      t.timestamps null: false
      t.string :class_name, null: false
    end

    create_table :job_run_statuses do |t|
      t.timestamps null: false
      t.string :label, null: false
      t.string :name, null: false
    end

    create_table :job_runs do |t|
      t.timestamps null: false
      t.references :jobs, index: true, null: false
      t.references :job_statuses, index: true, null: false
      t.datetime :run_start_time, :run_end_time, :batch_date
      t.integer :num_rows_success, :num_rows_error
    end
  end
end
