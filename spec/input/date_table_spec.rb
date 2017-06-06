RSpec.describe "DateTable" do
  describe ".each_row" do
    context "given start date and end date build consecutive rows" do
      it "Build two consecutive rows from 11/22/2017 to 11/24/2017" do
        start_date = Date.new(2017, 11, 22)
        end_date = Date.new(2017, 11, 24)
        dt = ETL::Input::DateTable.new(start_date, end_date)
        rows = dt.each_row().collect{|x| x}
        rows.each {|x| puts x}
        expect(rows.length).to eq 3
      end
      it "Build rows with end_date before start_date" do
        start_date = Date.new(2017, 11, 25)
        end_date = Date.new(2017, 11, 24)
        dt = ETL::Input::DateTable.new(start_date, end_date)
        rows = dt.each_row().collect{|x| x}
        rows.each {|x| puts x}
        expect(rows.length).to eq 0
      end
    end
  end
end

RSpec.describe "Day" do
  describe ".build_day" do
    context "given date build date row with info" do
      it "weekday date row should map info from 11/22/2017 to day" do
        d = Date.new(2017, 11, 22)
        v = ETL::Input::Day.new(d)
        expect(v.full_date).to eq '2017/11/22'
        expect(v.day_of_week_number).to eq 3
        expect(v.day_of_week_name).to eq "Wednesday"
        expect(v.day_of_month).to eq 22
        expect(v.day_of_year).to eq 326
        expect(v.weekday_flag).to eq true
        expect(v.weekend_flag).to eq false
        expect(v.week_number).to eq 47
        expect(v.month_number).to eq 11
        expect(v.month_name).to eq "November"
        expect(v.quarter).to eq 4
        expect(v.year).to eq 2017
        expect(v.year_month).to eq "2017/11"
        expect(v.year_quarter).to eq "2017/Q4"
      end

      it "weekend date row should map info from 11/26/2017 to day" do
        d = Date.new(2017, 11, 26)
        v = ETL::Input::Day.new(d)
        expect(v.full_date).to eq '2017/11/26'
        expect(v.day_of_week_number).to eq 0
        expect(v.day_of_week_name).to eq "Sunday"
        expect(v.day_of_month).to eq 26
        expect(v.day_of_year).to eq 330
        expect(v.weekday_flag).to eq false
        expect(v.weekend_flag).to eq true
        expect(v.week_number).to eq 47
        expect(v.month_number).to eq 11
        expect(v.month_name).to eq "November"
        expect(v.quarter).to eq 4
        expect(v.year).to eq 2017
        expect(v.year_month).to eq "2017/11"
        expect(v.year_quarter).to eq "2017/Q4"
      end
    end
  end
end
