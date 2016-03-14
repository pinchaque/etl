#!/usr/bin/env ruby
require File.expand_path("../../lib", __FILE__) + "/etl"
require 'etl/process/config_check'
ETL::Process::ConfigCheck.new.start
