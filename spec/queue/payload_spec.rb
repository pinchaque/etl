require 'etl/queue/payload.rb'
RSpec.describe "payload" do

  it "makes round trip" do    
    id = 123
    batch = {:foo => "abc", :bar => "xyz"}
    
    p = ETL::Queue::Payload.new
    p.job_id = id
    p.batch = batch
    expect(p.to_s).to eq(<<STR.strip)
Payload[job_id=123, batch={:foo=>\"abc\", :bar=>\"xyz\"}]
STR
    enc = p.encode
    expect(enc).to eq(<<STR.strip)
{\"batch\":{\"foo\":\"abc\",\"bar\":\"xyz\"},\"job_id\":123}
STR

    p2 = ETL::Queue::Payload.decode(enc)
    expect(p2.job_id).to eq(id)
    expect(p2.batch).to eq(batch)
  end
end
