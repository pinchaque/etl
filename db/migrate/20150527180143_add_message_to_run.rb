class AddMessageToRun < ActiveRecord::Migration
  def change
    add_column :job_runs, :message, :string
  end
end
