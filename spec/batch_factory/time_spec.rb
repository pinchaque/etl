
RSpec.describe "batch_factory/time" do
  
  describe "year - UTC - hour lag" do
    let(:tz) { 'UTC' }
    let(:lag) { 3600 }
    let(:tm) { ETL::BatchFactory::Year.new(tz, lag) }
    
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2013, 12, 31, 23, 59, 59) => ::Time.gm(2013, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 0) => ::Time.gm(2013, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2013, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 59, 59) => ::Time.gm(2013, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 1, 0, 0) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 1, 0, 1) => ::Time.gm(2014, 1, 1, 0, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ year: exp })
        end
      end
    end
  end
    
  describe "quarter - UTC - no lag" do
    let(:tz) { 'UTC' }
    let(:lag) { 0 }
    let(:tm) { ETL::BatchFactory::Quarter.new(tz, lag) }
    
    d = {
      ::Time.gm(2016, 1, 1, 0, 0, 0) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2016, 2, 13, 10, 9, 50) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 1, 0, 0, 0),
      ::Time.gm(2016, 12, 31, 23, 59, 59) => ::Time.gm(2016, 10, 1, 0, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ quarter: exp })
        end
      end
    end
  end
    
  describe "month - UTC - no lag" do
    let(:tz) { 'UTC' }
    let(:lag) { 0 }
    let(:tm) { ETL::BatchFactory::Month.new(tz, lag) }
    
    d = {
      ::Time.gm(2016, 1, 1, 0, 0, 0) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2016, 2, 13, 10, 9, 50) => ::Time.gm(2016, 2, 1, 0, 0, 0),
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 3, 1, 0, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 1, 0, 0, 0),
      ::Time.gm(2016, 12, 31, 23, 59, 59) => ::Time.gm(2016, 12, 1, 0, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ month: exp })
        end
      end
    end
  end
  
  describe "hour - America/Los_Angeles - no lag" do
    let(:tz) { 'America/Los_Angeles' }
    let(:lag) { 0 }
    let(:tm) { ETL::BatchFactory::Hour.new(tz, lag) }
    
    d = {
      # 9am UTC = 1am PST
      ::Time.utc(2016, 3, 10, 9, 0, 0) => ::Time.utc(2016, 3, 10, 1, 0, 0),
      ::Time.utc(2016, 3, 10, 9, 0, 59) => ::Time.utc(2016, 3, 10, 1, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 0, 0) => ::Time.utc(2016, 3, 10, 2, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 1, 0) => ::Time.utc(2016, 3, 10, 2, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 15, 15) => ::Time.utc(2016, 3, 10, 2, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ hour: exp })
        end
      end
    end
  end
  
  describe "hour - America/Los_Angeles - 1m lag" do
    let(:tz) { 'America/Los_Angeles' }
    let(:lag) { 60 }
    let(:tm) { ETL::BatchFactory::Hour.new(tz, lag) }
    
    d = {
      # 1am PST = 8am UTC
      ::Time.utc(2016, 3, 10, 9, 59, 59) => ::Time.utc(2016, 3, 10, 1, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 0, 0) => ::Time.utc(2016, 3, 10, 1, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 0, 59) => ::Time.utc(2016, 3, 10, 1, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 1, 0) => ::Time.utc(2016, 3, 10, 2, 0, 0),
      ::Time.utc(2016, 3, 10, 10, 15, 15) => ::Time.utc(2016, 3, 10, 2, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ hour: exp })
        end
      end
    end
  end
  
  describe "hour - America/Los_Angeles - no lag - DST boundaries" do
    let(:tz) { 'America/Los_Angeles' }
    let(:lag) { 0 }
    let(:tm) { ETL::BatchFactory::Hour.new(tz, lag) }
    
    d = {
      # Daylight Saving time began on 2016-03-13 @ 2am
      # 8:59am UTC = 1:59am PST, then we sprang forward
      ::Time.gm(2016, 3, 13, 9, 0, 0) => ::Time.gm(2016, 3, 13, 1, 0, 0),
      ::Time.gm(2016, 3, 13, 9, 59, 59) => ::Time.gm(2016, 3, 13, 1, 0, 0),
      # 10am UTC = 3am PST
      ::Time.gm(2016, 3, 13, 10, 0, 0) => ::Time.gm(2016, 3, 13, 3, 0, 0),
      ::Time.gm(2016, 3, 13, 10, 59, 59) => ::Time.gm(2016, 3, 13, 3, 0, 0),
      ::Time.gm(2016, 3, 13, 11, 0, 0) => ::Time.gm(2016, 3, 13, 4, 0, 0),
      
      # Daylight Saving time ended on 2015-11-11 @ 2am
      # 8:59am UTC = 1:59am PST, then we fall back
      ::Time.gm(2015, 11, 1, 8, 59, 59) => ::Time.gm(2015, 11, 1, 1, 0, 0),
      ::Time.gm(2015, 11, 1, 9, 0, 0) => ::Time.gm(2015, 11, 1, 1, 0, 0),
      ::Time.gm(2015, 11, 1, 9, 59, 59) => ::Time.gm(2015, 11, 1, 1, 0, 0),
      ::Time.gm(2015, 11, 1, 10, 0, 0) => ::Time.gm(2015, 11, 1, 2, 0, 0),
    }
  
    d.each do |curr_time, exp|
      it curr_time do
        pretend_now_is(curr_time) do
          expect(tm.generate).to eq({ hour: exp })
        end
      end
    end
  end
end
