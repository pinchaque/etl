#!/bin/ruby

# Pre-define the module so we can use simpler syntax
module ETL
end

# Core classes
require 'etl/jobs/result.rb'
require 'etl/jobs/base.rb'

# Various ETL jobs
require 'etl/jobs/success.rb'
