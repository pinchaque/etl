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

class JobRunStatus < ActiveRecord::Base

  # Returns the status ID given the label as a symbol
  def self.id_from_label(label)
    o = self.find_by!(label: label.to_s())
    if o.nil?
      return nil
    else
      return o.id
    end
  end

  # Returns the status label as a symbol given an ID
  def self.label_from_id(id)
    o = self.find(id)
    if o.nil?
      return nil
    else
      return o.label.to_sym()
    end
  end
end
