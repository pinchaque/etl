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

# Pre-define the module so we can use simpler syntax
module ETL
end

# Core classes
require 'etl/logger.rb'
require 'etl/jobs/result.rb'
require 'etl/jobs/base.rb'
require 'etl/schema/table.rb'
require 'etl/jobs/batch.rb'

# Various ETL jobs
require 'etl/jobs/dummy.rb'
require 'etl/jobs/csv.rb'
