require 'rails_helper'

require 'etl/core'

class TestCsvCreate < ETL::Job::CSV
  def initialize
    super
    @feed_name = "test_1"
    @input_file = "#{Rails.root}/spec/data/simple1.csv"
    @header = true
    @schema = ETL::Schema::Table.new(
      "day" => :date,
      "condition" => :string,
      "value_int" => :int,
      "value_num" => ETL::Schema::Type.new(:numeric, 10, 1),
      "value_float" => :float
    )
  end
end



RSpec.describe Job, :type => :job do

  it "csv - simple overwrite" do

    # remove old file
    outfile = "/var/tmp/etl_test_output/test_1/2015-03-31.csv"
    File.delete(outfile) if File.exist?(outfile)
    expect(File.exist?(outfile)).to be false

    job = TestCsvCreate.new
    batch = ETL::Job::DateBatch.new(2015, 3, 31)

    expect(job.output_file(batch)).to eq(outfile)

    jr = job.run(batch)

    expect(jr.status).to eq(:success)
    expect(jr.num_rows_success).to eq(3)
    expect(jr.num_rows_error).to eq(0)
    expect(jr.message).to include(outfile)
    expect(File.exist?(outfile)).to be true

    contents = IO.read(outfile)
    expect_contents = <<END
day,condition,value_int,value_num,value_float
2015-04-01,rain,0,12.3,59.3899
2015-04-02,snow,1,13.1,60.2934
2015-04-03,sun,-1,0.4,-12.83
END
    expect(contents).to eq(expect_contents)
  end
end
