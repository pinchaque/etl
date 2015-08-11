
require 'etl/core'

RSpec.describe "transforms" do

  it "map to nil" do
    t = ETL::Transform::MapToNil.new("", "[NULL]", -1)
    expect(t.transform("")).to be_nil
    expect(t.transform(-1)).to be_nil
    expect(t.transform("[NULL]")).to be_nil
    expect(t.transform("[null]")).to eq("[null]")
    expect(t.transform(1)).to eq(1)
  end
end
