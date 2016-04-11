
require 'etl/core'

RSpec.describe "input transforms" do

  it "input transforms" do
    data = [
      { "day" => "2015-04-01 10:03:04", "id" => "", "zip_code" => "501" },
      { "day" => "2015-04-02 23:45:01", "id" => "1", "zip_code" => "92101" },
      { "day" => "2015-04-03", "id" => "2", "zip_code" => "98101-3437" },
      { "day" => "2015-04-04", "id" => "3", "zip_code" => "" },
    ]

    input = ETL::Input::Array.new(data)

    input.row_transform = Proc.new do |row|
      t = ETL::Transform::MapToNil.new("")
      row.each do |k, v|
        row[k] = t.transform(v)
      end
    end
    
    # We can use lambda functions to transform
    # or chain together transform classes
    input.col_transforms = {
      "day" => ETL::Transform::DateTrunc.new(:day),
      "zip_code" => ETL::Transform::Zip5.new,
    }

    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01 00:00:00')
        expect(row['id']).to be_nil
        expect(row['zip_code']).to eq('00501')
      when 1 then
        expect(row['day']).to eq('2015-04-02 00:00:00')
        expect(row['id']).to eq("1")
        expect(row['zip_code']).to eq('92101')
      when 2 then
        expect(row['day']).to eq('2015-04-03 00:00:00')
        expect(row['id']).to eq("2")
        expect(row['zip_code']).to eq('98101')
      when 3 then
        expect(row['day']).to eq('2015-04-04 00:00:00')
        expect(row['id']).to eq("3")
        expect(row['zip_code']).to be_nil
      else
      end
      i += 1
    end

    expect(input.rows_processed).to eq(4)
  end
end
