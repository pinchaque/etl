require 'etl/queue/payload.rb'
RSpec.describe "payload" do

  it "makes round trip" do    
    id = 123
    batch = ETL::Batch.new({:foo => "abc", :bar => "xyz"})
    
    p = ETL::Queue::Payload.new(id, batch)
    expect(p.to_s).to eq(<<STR.strip)
Payload<job_id=123, batch={:bar=>"xyz", :foo=>"abc"}>
STR
    enc = p.encode
    expect(enc).to eq('{"batch":{"bar":"xyz","foo":"abc"},"job_id":123}')

    p2 = ETL::Queue::Payload.decode(enc)
    expect(p2.job_id).to eq(id)
    expect(p2.batch_hash).to eq(batch.to_h)
  end
end
