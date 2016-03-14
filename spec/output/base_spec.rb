
require 'etl/core'


RSpec.describe "base output" do

  it "base job id" do

    b = ETL::Output::Base.new
    
    d = [
      [{ :day => "2015-04-01" }, "20150401"],
      [{ :day => "New York" }, "newyork"],
      [{ :day => "Random Identifier #123  " }, "randomidentifier123"],
      [{ :day => "2015-04-01", :city => "New York" }, "newyork_20150401"],
      [{ :city => "New York", :day => "2015-04-01" }, "newyork_20150401"],
      [{ :color => "cyan", :city => "New York", :day => "2015-04-01" }, "newyork_cyan_20150401"],
    ]
    
    d.each do |a|
      expect(a.length).to eq(2)
      b.batch = ETL::Batch.new(a[0])
      expect(b.batch_id).to eq(a[1])
    end
  end
end
