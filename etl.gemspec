# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'etl/version'

Gem::Specification.new do |spec|
  spec.name          = "etl"
  spec.version       = ETL::VERSION
  spec.authors       = ["Chuck Smith"]
  spec.email         = ["pinchaque@gmail.com"]

  spec.summary       = "Framework for scheduling and running ETL jobs"
  spec.homepage      = "https://github.com/pinchaque/etl"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = "etl"
  spec.require_paths = ["lib"]

  # Removing as this is blocking chef
  # deployments
  #spec.add_dependency 'ruby-odbc'
  spec.add_dependency 'clamp'
  spec.add_dependency 'mysql2'
  spec.add_dependency 'sequel'
  spec.add_dependency 'tzinfo'
  spec.add_dependency 'tzinfo-data'
  spec.add_dependency 'influxdb'
  spec.add_dependency 'pg'
  spec.add_dependency 'aws-sdk'
  spec.add_dependency 'slack-notifier'

  spec.add_development_dependency 'bunny'
  spec.add_development_dependency 'factory_girl'
  spec.add_development_dependency 'forgery'
  spec.add_development_dependency 'rspec-core'
  spec.add_development_dependency 'rspec-expectations'
  spec.add_development_dependency 'rspec-mocks'
  spec.add_development_dependency 'time-warp'
end
