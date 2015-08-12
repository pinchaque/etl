#!/usr/bin/env ruby
require File.expand_path("../../lib", __FILE__) + "/etl"
ETL::Process::Worker.new.start
