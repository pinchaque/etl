class WeatherBase < ActiveRecord::Migration
  def change
    create_table :stations do |t|
      t.string :name, null: false
      t.float :latitude, null: false
      t.float :longitude, null: false
      t.float :elevation
      t.string :state_code
      t.string :full_name
      t.boolean :gsn_flag
      t.boolean :hcn_flag
      t.string :wmo_id
    end
    
    create_table :observations do |t|
      t.string :station_name, null: false
      t.datetime :date, null: false
      t.string :observation_type, null: false
      t.float :observation_value, null: false
   end
  end
end
