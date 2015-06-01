require 'rails_helper'
require 'etl/core'


RSpec.describe ETL::Job::DateBatch, :type => :job do

  it "batch constructor" do
    # construct from Date object
    a1 = ETL::Job::DateBatch.new(Date.new(2015, 3, 31))
    expect(a1.to_str()).to eq('2015-03-31') 
    expect(a1.to_s()).to eq('2015-03-31') 
    
    # construct from y, m, d
    a3 = ETL::Job::DateBatch.new(2015, 3, 31)
    expect(a3.to_str()).to eq('2015-03-31') 
    expect(a3.to_s()).to eq('2015-03-31') 

    # check attributes
    expect(a3.date.year).to eq(2015)
    expect(a3.date.month).to eq(3)
    expect(a3.date.day).to eq(31)

    # current date
    a4 = ETL::Job::DateBatch.new
    expect(a4.to_str()).to eq(Date.new.strftime('%F'))
    expect(a4.to_s()).to eq(Date.new.strftime('%F'))
  end
end
