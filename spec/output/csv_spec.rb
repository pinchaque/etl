
require 'etl/core'

# Test reading and writing basic CSV file
class TestCsvCreate1 < ETL::Output::CSV
  def initialize(params = {})
    super

    define_schema do |s|
      s.date("day")
      s.string("condition")
      s.int("value_int")
      s.numeric("value_num", 10, 1)
      s.float("value_float")
    end
  end
  
  def feed_name
    "test_1"
  end
end


# Test reading in pipe-separated file and outputting @ separated
class TestCsvCreate2 < ETL::Output::CSV
  def initialize(params = {})
    super({load_strategy: :insert_table}.merge(params))
    define_schema do |s|
      s.date("day")
      s.string("condition")
      s.int("value_int")
      s.numeric("value_num", 10, 1)
      s.float("value_float")
    end
  end
  
  def feed_name
    "test_2"
  end

  def csv_output_options
    return super.merge({col_sep: '@'})
  end
end



RSpec.describe "csv output" do

  it "csv - overwrite" do

    # remove old file
    outfile = "/var/tmp/etl_test_output/test_1/20150331.csv"
    File.delete(outfile) if File.exist?(outfile)
    expect(File.exist?(outfile)).to be false

    input = ETL::Input::CSV.new({file: "#{ETL.root}/spec/data/simple1.csv"})
    input.headers_map = {
        "attribute" => "condition", 
        "value_numeric" => "value_num"
    }
    batch = ETL::Batch.new({ :day => "2015-03-31" })

    job = TestCsvCreate1.new({load_strategy: :insert_table})
    job.reader = input
    job.batch = batch
    jr = job.run
    expect(job.output_file).to eq(outfile)
    expect(jr.rows_processed).to eq(3)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true
    expect(input.rows_processed).to eq(3)

    contents = IO.read(outfile)
    expect_contents = <<END
day,condition,value_int,value_num,value_float
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
END
    expect(contents).to eq(expect_contents)

    # run a second time
    job = TestCsvCreate1.new({load_strategy: :insert_table})
    job.reader = input
    job.batch = batch
    jr = job.run
    expect(job.output_file).to eq(outfile)
    expect(jr.rows_processed).to eq(3)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true
    expect(input.rows_processed).to eq(3)

    contents = IO.read(outfile)
    expect_contents = <<END
day,condition,value_int,value_num,value_float
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
END
    expect(contents).to eq(expect_contents)

  end


  it "csv - append" do

    # remove old file
    outfile = "/var/tmp/etl_test_output/test_1/20150331.csv"
    File.delete(outfile) if File.exist?(outfile)
    expect(File.exist?(outfile)).to be false

    input = ETL::Input::CSV.new({file: "#{ETL.root}/spec/data/simple1.csv"})
    input.headers_map = {
        "attribute" => "condition", 
        "value_numeric" => "value_num"
    }
    batch = ETL::Batch.new({ :day => "2015-03-31" })

    job = TestCsvCreate1.new({load_strategy: :insert_append})
    job.reader = input
    job.batch = batch
    jr = job.run

    expect(job.output_file).to eq(outfile)
    expect(jr.rows_processed).to eq(3)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true
    expect(input.rows_processed).to eq(3)

    contents = IO.read(outfile)
    expect_contents = <<END
day,condition,value_int,value_num,value_float
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
END
    expect(contents).to eq(expect_contents)

    # run a second time
    job = TestCsvCreate1.new({load_strategy: :insert_append})
    job.reader = input
    job.batch = batch
    jr = job.run
    expect(job.output_file).to eq(outfile)
    expect(jr.rows_processed).to eq(3)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true
    expect(input.rows_processed).to eq(3)

    contents = IO.read(outfile)
    expect_contents = <<END
day,condition,value_int,value_num,value_float
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
END
    expect(contents).to eq(expect_contents)

  end


  it "psv - overwrite" do

    # remove old file
    outfile = "/var/tmp/etl_test_output/test_2/20150331.csv"
    File.delete(outfile) if File.exist?(outfile)
    expect(File.exist?(outfile)).to be false
    # file does not have headers

    input = ETL::Input::CSV.new({
      file: "#{ETL.root}/spec/data/simple1.psv",
      headers: false, 
      col_sep: '|'})
    input.headers = %w{day condition value_int value_num value_float}
    job = TestCsvCreate2.new
    job.reader = input
    batch = ETL::Batch.new({ :day => "2015-03-31" })

    job.batch = batch
    jr = job.run

    expect(input.rows_processed).to eq(3)
    expect(job.output_file).to eq(outfile)
    expect(jr.rows_processed).to eq(3)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true

    contents = IO.read(outfile)
    expect_contents = <<END
day@condition@value_int@value_num@value_float
2015-04-01@rain@0@12.3@59.3899
2015-04-02@snow@1@13.1@60.2934
2015-04-03@sun@-1@0.4@-12.83
END
    expect(contents).to eq(expect_contents)
  end
end
