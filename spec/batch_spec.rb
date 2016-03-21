require 'json'

RSpec.describe "batch" do

  it "creates batch" do
    h = { foo: "bar", quux: 123 }
    b = ETL::Batch.new(h)
    expect(b.to_h).to eq(h)
    expect(b.to_json).to eq('{"foo":"bar","quux":123}')
    expect(b.to_s).to eq('bar_123')
  end
  
  it "batch has deterministic ordering" do
    b1 = ETL::Batch.new({ foo: "bar", quux: 123 })
    b2 = ETL::Batch.new({ quux: 123, foo: "bar" })
    expect(b1.to_h).to eq(b2.to_h)
    expect(b1.to_json).to eq(b2.to_json)
    expect(b1.to_s).to eq(b2.to_s)
  end
  
  it "changing batch's hash doesn't change the ordering" do
    h = { foo: "bar", quux: 123 }
    b = ETL::Batch.new(h)
    
    # original state
    expect(b.to_h).to eq(h)
    expect(b.to_json).to eq('{"foo":"bar","quux":123}')
    expect(b.to_s).to eq('bar_123')
    
    # change the hash
    x = b.to_h
    x[:changed] = 456
    x[:quux] = 789
    
    # should still be in original state
    expect(b.to_h).to eq(h)
    expect(b.to_json).to eq('{"foo":"bar","quux":123}')
    expect(b.to_s).to eq('bar_123')
  end
end
