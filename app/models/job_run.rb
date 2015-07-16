###############################################################################
# Copyright (C) 2015 Chuck Smith
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
###############################################################################

require 'json'

class JobRun < ActiveRecord::Base
  belongs_to :job
  attr_accessor :job

  # Creates JobRun object from Job and batch date
  def self.create_for_job(job, batch)
    JobRun.new do |jr|
      jr.job = job
      jr.job_id = job.id
      jr.status = :new
      jr.batch = batch.to_json
    end
  end

  # Sets status for this job given label
  def status=(label)
    self.job_run_status_id = JobRunStatus.id_from_label(label)
  end

  # Gets status for this job as the label
  def status()
    JobRunStatus.label_from_id(self.job_run_status_id)
  end

  # Sets the current status as running and initializes run_start_time
  def running()
    self.status = :running
    self.run_start_time = DateTime.now
    save()
  end

  # Sets the final status as success along with rows affected
  def success(result)
    final_state(:success, result)
  end

  # Sets the final status as error along with rows affected
  def error(result)
    final_state(:error, result)
  end

  private
  def final_state(state, result)
    self.status = state
    self.run_end_time = DateTime.now
    self.num_rows_success = result.num_rows_success
    self.num_rows_error = result.num_rows_error
    self.message = result.message
    save()
  end

end
