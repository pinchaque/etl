RSpec.describe "FiscalQuarter" do
  describe ".quarter_lookup" do
    context "given start month being July verify quarters and month nums are correct" do
      it "Ensure fiscal quarter is correct start month > 1" do
        fq = ETL::Input::FiscalQuarter.new(6)
        expected = {6 => 1, 7 => 1, 8 => 1, 9=> 2, 10=> 2, 11=>2, 12=>3, 1=>3, 2=>3, 3=> 4, 4=> 4, 5=> 4}
        expected_mon_num = {6 => 1, 7 => 2, 8 => 3, 9=> 1, 10=> 2, 11=>3, 12=>1, 1=>2, 2=>3, 3=> 1, 4=> 2, 5=> 3}
        for i in 1..12
          expect(fq.quarter_lookup[i]).to eq expected[i]
          expect(fq.quarter_month_num_lookup[i]).to eq expected_mon_num[i]
        end
      end
      it "Ensure fiscal quarter is correct start month == 1" do
        fq = ETL::Input::FiscalQuarter.new(1)
        expected = {1 => 1, 2 => 1, 3 => 1, 4=> 2, 5=> 2, 6=>2, 7=>3, 8=>3, 9=>3, 10=> 4, 11=> 4, 12=> 4}
        expected_mon_num = {1 => 1, 2 => 2, 3 => 3, 4=> 1, 5=> 2, 6=>3, 7=>1, 8=>2, 9=>3, 10=> 1, 11=> 2, 12=> 3}
        for i in 1..12
          expect(fq.quarter_lookup[i]).to eq expected[i]
          expect(fq.quarter_month_num_lookup[i]).to eq expected_mon_num[i]
        end
      end
    end
  end
end

RSpec.describe "DateTable" do
  describe ".each_row" do
    context "given start date and end date build consecutive rows" do
      it "Build two consecutive rows, ensure fiscal year correct" do
        start_date = Date.new(2017, 10, 01)
        end_date = Date.new(2018, 12, 01)
        dt = ETL::Input::DateTable.new(11, start_date, end_date)
        days= []
        rows = dt.each_row do |d|
          days << d
        end
        expect(days.length).to eq 427
        row = days[0]
        expect(row.fiscal_year).to eq 2016
        row = days[40]
        expect(row.fiscal_year).to eq 2017
        row = days[396]
        expect(row.full_date).to eq "2018/11/01"
        expect(row.fiscal_year).to eq 2018
      end
      it "Build rows with end_date before start_date" do
        start_date = Date.new(2017, 11, 25)
        end_date = Date.new(2017, 11, 24)
        dt = ETL::Input::DateTable.new(1, start_date, end_date)
        days= []
        rows = dt.each_row do |d|
          days << d
        end
        expect(days.length).to eq 0
      end
    end
  end
end

RSpec.describe "Day" do
  describe ".build_day" do
    context "given date build date row with info" do
      it "Weekday with Quarter starting in Jan" do
        d = Date.new(2017, 11, 22)
        fq = ETL::Input::FiscalQuarter.new(1)
        v = ETL::Input::Day.new(1, d, fq)
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
        expect(v.fiscal_year).to eq 2017
        expect(v.fiscal_quarter).to eq "2017/Q4"
        expect(v.fiscal_quarter_month).to eq 2
      end

      it "Weekend with Quarter starting in Dec" do
        d = Date.new(2017, 11, 26)
        fq = ETL::Input::FiscalQuarter.new(12)
        v = ETL::Input::Day.new(12, d, fq)
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
        expect(v.fiscal_year).to eq 2016
        expect(v.fiscal_quarter).to eq "2016/Q4"
        expect(v.fiscal_quarter_month).to eq 3
      end
    end
  end
end
