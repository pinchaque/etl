
require 'etl/core'


RSpec.describe "inputs" do

  it "csv input each" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    fname = "#{ETL.root}/spec/data/simple1.csv"
    input = ETL::Input::CSV.new(fname)

    expect(input.name).to eq("CSV file '#{fname}'")

    i = 0
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)

    # test second iteration through the file
    input.each_row do |row|
      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
      end
      i += 1
    end
    expect(input.rows_processed).to eq(3)
   end

  it "csv input batch 1" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")

    i = 0
    input.each_row_batch(1) do |row_ary|
      expect(row_ary.length).to eq(1)
      row = row_ary[0]

      case i
      when 0 then
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')
      when 1 then
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 2 then
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "csv input batch 2" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")

    i = 0
    input.each_row_batch(2) do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(2)

        row = row_ary[0]
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')

        row = row_ary[1]
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')
      when 1 then
        expect(row_ary.length).to eq(1)

        row = row_ary[0]
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "csv input batch default" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')

        row = row_ary[1]
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')

        row = row_ary[2]
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "csv input with file headers given header array" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")
    input.headers = %w{one two three four five}

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row.keys.length).to eq(5)
        input.headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-01')
        expect(row['two']).to eq('rain')

        row = row_ary[1]
        expect(row.keys.length).to eq(5)
        input.headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-02')
        expect(row['two']).to eq('snow')

        row = row_ary[2]
        expect(row.keys.length).to eq(5)
        input.headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-03')
        expect(row['two']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "csv input with file headers given headers hash" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")
    input.headers_map = {"attribute" => "condition"}

    expected_headers = %w{day condition value_int value_numeric value_float}

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-01')
        expect(row['condition']).to eq('rain')

        row = row_ary[1]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-02')
        expect(row['condition']).to eq('snow')

        row = row_ary[2]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-03')
        expect(row['condition']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "csv input with file headers given array and hash" do
    # day,attribute,value_int,value_num,value_float
    # 2015-04-01,rain,0,12.3,59.3899
    # 2015-04-02,snow,1,13.1,60.2934
    # 2015-04-03,sun,-1,0.4,-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.csv")
    input.headers = %w{one two three four five}
    input.headers_map = {"two" => "condition"}


    expected_headers = %w{one condition three four five}

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-01')
        expect(row['condition']).to eq('rain')

        row = row_ary[1]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-02')
        expect(row['condition']).to eq('snow')

        row = row_ary[2]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['one']).to eq('2015-04-03')
        expect(row['condition']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

  it "psv input without file headers" do
    # 2015-04-01|rain|0|12.3|59.3899
    # 2015-04-02|snow|1|13.1|60.2934
    # 2015-04-03|sun|-1|0.4|-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.psv",
      {headers: false, col_sep: '|'})

    expected_headers = [0, 1, 2, 3, 4]

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row[0]).to eq('2015-04-01')
        expect(row[1]).to eq('rain')

        row = row_ary[1]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row[0]).to eq('2015-04-02')
        expect(row[1]).to eq('snow')

        row = row_ary[2]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row[0]).to eq('2015-04-03')
        expect(row[1]).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end


  it "psv input without file headers given array" do
    # 2015-04-01|rain|0|12.3|59.3899
    # 2015-04-02|snow|1|13.1|60.2934
    # 2015-04-03|sun|-1|0.4|-12.83
    input = ETL::Input::CSV.new("#{ETL.root}/spec/data/simple1.psv",
      {headers: false, col_sep: '|'})
    input.headers = %w{day attribute value_int value_numeric value_float}
    expected_headers = %w{day attribute value_int value_numeric value_float}

    i = 0
    input.each_row_batch do |row_ary|
      case i
      when 0 then
        expect(row_ary.length).to eq(3)

        row = row_ary[0]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-01')
        expect(row['attribute']).to eq('rain')

        row = row_ary[1]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-02')
        expect(row['attribute']).to eq('snow')

        row = row_ary[2]
        expect(row.keys.length).to eq(5)
        expected_headers.each() { |h| expect(row.has_key?(h)).to be true }
        expect(row['day']).to eq('2015-04-03')
        expect(row['attribute']).to eq('sun')
      else
        raise "Invalid batch index #{i} #{row_ary}"
      end
      i += 1
    end

    expect(input.rows_processed).to eq(3)
  end

end
