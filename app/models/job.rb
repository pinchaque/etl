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

class Job < ActiveRecord::Base
  has_many :job_runs

  # Register specified class as a job if it doesn't already exist
  def self.register(class_name)
    r = self.find_by(class_name: class_name)
    if r.nil?
        r = Job.new
        r.class_name = class_name
        r.save()
    end
    return r
  end

  # Creates JobRun object for this Job and specified batch
  def create_run(batch)
    jr = JobRun.create_for_job(self, batch)
    jr.save
    jr
  end
end
