
require 'etl/core'

RSpec.describe "transforms" do

  it "zip codes" do

    # array of inputs and expected outputs
    d = [
      ["92101", "92101"],
      ["  92101", "92101"],
      ["92101  ", "92101"],
      ["501", "00501"],
      ["50", "00050"],
      ["92101-3437", "92101"],
      ["92101-34378585", "92101"],
      ["92101-34378585", "92101"],
      ["921013437", "92101"],
      ["  921013437", "92101"],
      ["abcd", nil],
      ["92101xyz", nil],
      ["xx92101 ", nil],
      ["", nil],
    ]

    t = ETL::Transform::Zip5.new
    d.each do |a|
      expect(a.length).to eq(2)
      if a[1].nil?
        expect(t.transform(a[0])).to be_nil
      else
        expect(t.transform(a[0])).to eq(a[1])
      end
    end
  end
end
