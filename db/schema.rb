# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20140929020909) do

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "observations", force: true do |t|
    t.string   "station_name",      null: false
    t.datetime "date",              null: false
    t.string   "observation_type",  null: false
    t.float    "observation_value", null: false
  end

  create_table "stations", force: true do |t|
    t.string  "name",       null: false
    t.float   "latitude",   null: false
    t.float   "longitude",  null: false
    t.float   "elevation"
    t.string  "state_code"
    t.string  "full_name"
    t.boolean "gsn_flag"
    t.boolean "hcn_flag"
    t.string  "wmo_id"
  end

end
