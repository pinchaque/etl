require 'json'

RSpec.describe "batch" do

  it "creates batch" do
    h = { foo: "bar", quux: 123 }
    b = ETL::Batch.new(h)
    expect(b.to_h).to eq(h)
    expect(b.to_json).to eq('{"foo":"bar","quux":123}')
    expect(b.to_s).to eq('bar_123')
  end
end
