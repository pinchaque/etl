
# Test class that generates an array of batches
class SingleBatchSpec < ETL::BatchFactory::Base
  def generate
    ETL::Batch.new({num: 0})
  end
end

# Test class that generates an array of batches
class MultiBatchSpec < ETL::BatchFactory::Base
  def generate
    (1..5).map{ |i| ETL::Batch.new({num: i}) }
  end
end

RSpec.describe "batch_factory/multi" do
  
  it "single batch works" do
    r = SingleBatchSpec.new.map { |b| b.to_h }
    expect(r).to eq([{num: 0}])
  end
  
  it "multi batch works" do
    r = MultiBatchSpec.new.map { |b| b.to_h }
    expect(r).to eq((1..5).map{ |i| {num: i} })
  end
end
