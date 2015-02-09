# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ name: 'Chicago' }, { name: 'Copenhagen' }])
#   Mayor.create(name: 'Emanuel', city: cities.first)

JobRunStatus.create([
  { label: :new, name: "New" },
  { label: :blocked, name: "Blocked on Dependencies" },
  { label: :ready, name: "Ready to Run" },
  { label: :running, name: "Running" },
  { label: :success, name: "Success" },
  { label: :error, name: "Error" },
])
