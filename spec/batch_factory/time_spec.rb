
RSpec.describe "batch_factory/time" do
  
  describe "batch factories hash geneation" do
    curr_time = ::Time.gm(2016, 5, 13, 10, 9, 50) 
    {
      ETL::BatchFactory::Year => { time: ::Time.gm(2016, 1, 1, 0, 0, 0) },
      ETL::BatchFactory::Quarter => { time: ::Time.gm(2016, 4, 1, 0, 0, 0) },
      ETL::BatchFactory::Month => { time: ::Time.gm(2016, 5, 1, 0, 0, 0) },
      ETL::BatchFactory::Day => { time: ::Time.gm(2016, 5, 13, 0, 0, 0) },
      ETL::BatchFactory::Hour => { time: ::Time.gm(2016, 5, 13, 10, 0, 0) },
    }.each do |klass, exp|
      it klass do
        pretend_now_is(curr_time) do
          expect(klass.new.generate.to_h).to eq(exp)
        end
      end
    end
  end
  
  describe "passes validation" do
    {
      ETL::BatchFactory::Year => [
        { time: ::Time.gm(2016, 1, 1, 0, 0, 0) },
        { time: ::Time.gm(1999, 1, 1, 0, 0, 0) },
      ],
      ETL::BatchFactory::Quarter => [
        { time: ::Time.gm(2012, 1, 1, 0, 0, 0) },
        { time: ::Time.gm(2013, 4, 1, 0, 0, 0) },
        { time: ::Time.gm(2014, 7, 1, 0, 0, 0) },
        { time: ::Time.gm(2016, 10, 1, 0, 0, 0) },
      ],
      ETL::BatchFactory::Month => (1..12).to_a.map { |m| { time: ::Time.gm(2016, m, 1, 0, 0, 0) } },
      ETL::BatchFactory::Day => (1..31).to_a.map { |d| { time: ::Time.gm(2016, 5, d, 0, 0, 0) } },
      ETL::BatchFactory::Hour => (0..23).to_a.map { |h| { time: ::Time.gm(2016, 5, 8, h, 0, 0) } },
    }.each do |klass, example_ary|
      describe klass do
        fact = klass.new
        example_ary.each do |hsh|
          it hsh do
            batch = ETL::Batch.new(hsh)
            fact.validate!(batch) # shouldn't raise exception
            expect(fact.validate(batch)).to_not be_nil
          end
        end
      end
    end
  end
  
  describe "fails validation" do
    {
      ETL::BatchFactory::Year => [
        { bad_label: ::Time.gm(1999, 1, 1, 0, 0, 0) },
        { time: ::Time.gm(2016, 2, 1, 0, 0, 0) },
        { time: ::Time.gm(2016, 1, 2, 0, 0, 0) },
        { time: ::Time.gm(2016, 1, 1, 1, 0, 0) },
        { time: ::Time.gm(2016, 1, 1, 0, 1, 0) },
        { time: ::Time.gm(2016, 1, 1, 0, 0, 1) },
      ],
      ETL::BatchFactory::Quarter => [
        { time: ::Time.gm(2012, 2, 1, 0, 0, 0) },
        { time: ::Time.gm(2012, 3, 1, 0, 0, 0) },
        { time: ::Time.gm(2013, 5, 1, 0, 0, 0) },
        { time: ::Time.gm(2013, 6, 1, 0, 0, 0) },
        { time: ::Time.gm(2014, 8, 1, 0, 0, 0) },
        { time: ::Time.gm(2014, 8, 1, 0, 0, 0) },
        { time: ::Time.gm(2016, 11, 1, 0, 0, 0) },
        { time: ::Time.gm(2016, 12, 1, 0, 0, 0) },
      ],
      ETL::BatchFactory::Month => (1..12).to_a.map { |m| { time: ::Time.gm(2016, m, 2, 0, 0, 0) } },
      ETL::BatchFactory::Day => (1..31).to_a.map { |d| { time: ::Time.gm(2016, 5, d, 1, 0, 0) } },
      ETL::BatchFactory::Hour => (0..23).to_a.map { |h| { time: ::Time.gm(2016, 5, 8, h, 1, 0) } },
    }.each do |klass, example_ary|
      describe klass do
        fact = klass.new
        example_ary.each do |hsh|
          it hsh do
            batch = ETL::Batch.new(hsh)
            expect { fact.validate!(batch) }.to raise_error(ETL::BatchError)
            expect(fact.validate(batch)).to be_nil
          end
        end
      end
    end
  end
  
  describe "parsing a hash adds Time objects", foo: true do
    # note that this doesn't do any rounding, just string => Time conversion
    # these will all fail validation
    [
      ETL::BatchFactory::Year,
      ETL::BatchFactory::Quarter,
      ETL::BatchFactory::Month,
      ETL::BatchFactory::Day,
      ETL::BatchFactory::Hour,
    ].each do |klass|
      fact = klass.new
      describe klass do
        {
          { time: "2016-03-03T23:10:05Z" } => { time: ::Time.gm(2016, 3, 3, 23, 10, 5) },
        }.each do |input_hash, exp_hash|
          it input_hash do
            batch = fact.from_hash(input_hash)
            expect(batch.to_h).to eq(exp_hash)
            expect { fact.validate!(batch) }.to raise_error(ETL::BatchError)
            expect(fact.validate(batch)).to be_nil
          end
        end
      end
    end
  end
  
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
          expect(tm.generate.to_h).to eq({ time: exp })
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
          expect(tm.generate.to_h).to eq({ time: exp })
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
          expect(tm.generate.to_h).to eq({ time: exp })
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
          expect(tm.generate.to_h).to eq({ time: exp })
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
          expect(tm.generate.to_h).to eq({ time: exp })
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
          expect(tm.generate.to_h).to eq({ time: exp })
        end
      end
    end
  end
end
