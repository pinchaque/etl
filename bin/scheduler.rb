#!/usr/bin/env ruby
require File.expand_path("../../lib", __FILE__) + "/etl"
require 'etl/process/scheduler'
ETL::Process::Scheduler.new.start
