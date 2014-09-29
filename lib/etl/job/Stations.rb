
class Etl::Job::Stations extends Etl::Job::Base



  def extract
    rules = {
      :name => { :min => 1, :max => 11},
      :latitude => { :min => 13, :max => 20},
      :longitude => { :min => 22, :max => 30},
      :elevation => { :min => 32, :max => 37},
      :state => { :min => 39, :max => 40},
      :full_name => { :min => 42, :max => 71},
      :gsn_flag => { :min => 73, :max => 75},
      :hcn_flag => { :min => 77, :max => 79},
      :wmo_id => { :min => 81, :max => 85},
    }

    r = FixedValueReader.new(rules)
    r.base(1)
    

    
    create temp table - what schema?

    r.each |row| do
      save to temp table
    end



  end

  def transform
  end

  def do_load
  end

end
