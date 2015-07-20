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

require 'psych'
require 'sequel'

ENV["ETL_ENV"] ||= 'development'

module ETL
  def ETL.root
    File.expand_path('../..', __FILE__)
  end
  
  def ETL.log_file
    "#{ETL.root}/log/#{ENV['ETL_ENV']}.log"
  end
  
  # returns array of DBs parsed from config file
  def ETL.db_config
    if ETL.respond_to?("db_config_file")
      fname = ETL.db_config_file
    else
      fname = "#{ETL.root}/config/database.yml"
    end
    Psych.load_file(fname)
  end
end

# Set up the database connection that's needed for Sequel models
# Also we can use the DB constant in the rest of the code
dbconfig = ETL.db_config[ENV["ETL_ENV"]]
DB = Sequel::Model.db = Sequel.connect(dbconfig)
    
# Now include the rest of code needed for ETL system
require 'etl/core'
