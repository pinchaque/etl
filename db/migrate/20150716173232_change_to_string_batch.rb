class ChangeToStringBatch < ActiveRecord::Migration
  def change
    add_column :job_runs, :batch, :string
    remove_column :job_runs, :batch_date, :datetime
  end
end
