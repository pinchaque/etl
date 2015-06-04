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

module ETL::Job

  # Base class for all ETL jobs
  class Base
    attr_accessor :feed_name, :schema, :input_file

    def model()
      # return the model if we already have it cached in this instance
      return @model unless @model.nil?

      # get the model out of the DB
      @model = Job.register(self.class.to_s())
    end

    def logger
      Rails.logger
    end

    # Runs the job for the batch, keeping the status updated and handling
    # exceptions.
    def run(batch)
      jr = model().create_run(batch)

      log_prefix = "[Job=#{model().class_name} / Batch=#{batch}] "

      begin
        logger.info(log_prefix + "Running...")
        jr.running()
        result = run_internal(batch)
        logger.info(log_prefix + "Success! #{result.num_rows_success} rows; "\
          + "#{result.num_rows_error} errors; #{result.message}")
        jr.success(result)
      rescue Exception => ex
        logger.error(log_prefix + "Error: #{ex}")
        ex.backtrace.each do |x|
          logger.error(log_prefix + "    #{x}")
        end
        result = Result.new
        result.message = ex.message
        jr.error(result)
      end

      return jr
    end
  end
end
