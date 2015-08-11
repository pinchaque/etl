
require 'etl/core'

RSpec.describe "transforms" do

  it "date trunc" do

    # array of inputs and expected outputs
    d = {
      "2015-04-01" => 
      {
        minute: "2015-04-01 00:00:00",
        hour: "2015-04-01 00:00:00",
        day: "2015-04-01 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },
      
      "2015-04-03 02:34:31" =>
      {
        minute: "2015-04-03 02:34:00",
        hour: "2015-04-03 02:00:00",
        day: "2015-04-03 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      "2015-04-03 11:34:31.23" =>
      {
        minute: "2015-04-03 11:34:00",
        hour: "2015-04-03 11:00:00",
        day: "2015-04-03 00:00:00",
        week: "2015-03-29 00:00:00",
        month: "2015-04-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      "2015-05-11" =>
      {
        minute: "2015-05-11 00:00:00",
        hour: "2015-05-11 00:00:00",
        day: "2015-05-11 00:00:00",
        week: "2015-05-10 00:00:00",
        month: "2015-05-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },

      # test to make sure week rounding works across years
      "2014-12-27 23:59:59" => { week: "2014-12-21 00:00:00" },
      "2014-12-28 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-29 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-30 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2014-12-31 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-01 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-02 00:00:00" => { week: "2014-12-28 00:00:00" },
      "2015-01-03 23:59:59" => { week: "2014-12-28 00:00:00" },
      "2015-01-04 00:00:00" => { week: "2015-01-04 00:00:00" },

      "abcd" =>
      {
        minute: nil,
        hour: nil,
        day: nil,
        week: nil,
        month: nil,
        quarter: nil,
        year: nil,
      },

      "" =>
      {
        minute: nil,
        hour: nil,
        day: nil,
        week: nil,
        month: nil,
        quarter: nil,
        year: nil,
      },

      DateTime.parse("2015-05-11 12:34:55") =>
      {
        minute: "2015-05-11 12:34:00",
        hour: "2015-05-11 12:00:00",
        day: "2015-05-11 00:00:00",
        week: "2015-05-10 00:00:00",
        month: "2015-05-01 00:00:00",
        quarter: "2015-04-01 00:00:00",
        year: "2015-01-01 00:00:00",
      },
    }

    d.each do |input, tests|
      #puts("*** #{input} ***")
      tests.each do |resolution, expected|
        #puts("--- #{resolution} => #{expected} ---")
        t = ETL::Transform::DateTrunc.new(resolution)
        if expected.nil?
          expect(t.transform(input)).to be_nil
        else
          expect(t.transform(input)).to eq(expected)
        end
      end
    end
  end
end
