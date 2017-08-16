require 'etl/queue/sqs.rb'

RSpec.describe "sqs-based queue" do

  def create_payload(id)
    ETL::Queue::Payload.new(id, ETL::Batch.new)
  end
  let(:queue_url) {  ENV["ETL_TEST_SQS_QUEUE_URL"] }
  let(:region) {  ENV["ETL_TEST_SQS_REGION"] }

  it "purges messages" do

    begin
      queue = ETL::Queue::SQS.new(url: queue_url, region: region)
      queue.purge
      sleep(60)
      expect(queue.message_count).to eq(0)

      (1..5).to_a.each do |i|
        queue.enqueue(create_payload(i))
      end
      sleep(20)
      expect(queue.message_count).to eq(5)
    ensure
      queue.purge
      sleep(60)
      expect(queue.message_count).to eq(0)
    end
  end

  it "processes messages" do
    queue = ETL::Queue::SQS.new(queue_url: queue_url, region: region, idle_timeout: 120)
    queue.purge
    sleep(60)
    expect(queue.message_count).to eq(0)

    (1..5).to_a.each do |i|
      queue.enqueue(create_payload(i))
    end

    j = 1
    thr = queue.process_async do |message_info, payload|
      queue.ack(message_info)
      j += 1
    end
    sleep(20)
    #ack should delete them.
    expect(queue.message_count).to eq(0)
    expect(j).to eq(6)
  end
end

