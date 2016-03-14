require 'util/time_util'

RSpec.describe "time_util" do

  describe "round_year" do
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2015, 12, 31, 23, 59, 59) => ::Time.gm(2015, 1, 1, 0, 0, 0),
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::TimeUtil.round_year(inp)).to eq(exp)
      end
    end
  end

  describe "round_quarter" do
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 1, 1, 0, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2015, 12, 31, 23, 59, 59) => ::Time.gm(2015, 10, 1, 0, 0, 0),
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::TimeUtil.round_quarter(inp)).to eq(exp)
      end
    end
  end

  describe "round_month" do
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 3, 1, 0, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 1, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2015, 12, 31, 23, 59, 59) => ::Time.gm(2015, 12, 1, 0, 0, 0),
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::TimeUtil.round_month(inp)).to eq(exp)
      end
    end
  end

  describe "round_day" do
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 3, 13, 0, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 13, 0, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2015, 12, 31, 23, 59, 59) => ::Time.gm(2015, 12, 31, 0, 0, 0),
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::TimeUtil.round_day(inp)).to eq(exp)
      end
    end
  end

  describe "round_hour" do
    d = {
      ::Time.gm(2016, 3, 13, 10, 9, 50) => ::Time.gm(2016, 3, 13, 10, 0, 0),
      ::Time.gm(2016, 4, 13, 10, 9, 50) => ::Time.gm(2016, 4, 13, 10, 0, 0),
      ::Time.gm(2014, 1, 1, 0, 0, 5) => ::Time.gm(2014, 1, 1, 0, 0, 0),
      ::Time.gm(2015, 12, 31, 23, 59, 59) => ::Time.gm(2015, 12, 31, 23, 0, 0),
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::TimeUtil.round_hour(inp)).to eq(exp)
      end
    end
  end
end
