require 'etl/queue/file.rb'

ENV["TMPDIR"] = "/var/tmp"
RSpec.describe "file-based queue" do
  
  def create_payload(id)
    ETL::Queue::Payload.new(id, ETL::Batch.new)
  end

  it "purges messages" do

    queue = ETL::Queue::File.new
    expect(queue.message_count).to eq(0)
    
    (1..5).to_a.each do |i|
      queue.enqueue(create_payload(i))
      expect(queue.message_count).to eq(i)
    end
    
    queue.purge
    expect(queue.message_count).to eq(0)
  end
  
  it "processes messages" do

    queue = ETL::Queue::File.new
    expect(queue.message_count).to eq(0)
    
    (1..5).to_a.each do |i|
      queue.enqueue(create_payload(i))
      expect(queue.message_count).to eq(i)
    end 
    
    j = 1
    thr = queue.process_async do |message_info, payload|
      expect(payload.job_id).to eq(j)
      queue.ack(message_info)
      j += 1
    end
    
    # give it a couple seconds to process
    ret = thr.join(2)
    expect(ret).to be_nil # it should time out
    thr.terminate

    expect(queue.message_count).to eq(0)
  end
end
