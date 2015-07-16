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

require 'sequel'

module ETL::Model
  class JobRunStatus < ::Sequel::Model

    # Returns the status ID given the label as a symbol
    def self.id_from_label(label)
      o = self.first(:label => label.to_s())
      o.nil? ? nil : o.id
    end

    # Returns the status label as a symbol given an ID
    def self.label_from_id(id)
      o = self.first(:id => id)
      o.nil? ? nil : o.label.to_sym()
    end
  end

  JobRunStatus.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
