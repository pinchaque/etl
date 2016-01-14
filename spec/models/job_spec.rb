
RSpec.describe "job model" do
  
  describe "output params to json" do
    [
      { inp: { "foo" => :bar, "quux" => 1 }, exp: '{"foo":"bar","quux":1}' },
      { inp: { "foo" => nil, "quux" => '' }, exp: '{"foo":null,"quux":""}' },
      { inp: nil, exp: nil},
    ].each do |x|
      it x[:inp] do
        j = ETL::Model::Job.new
        j.output_params_hash = x[:inp]
        
        if x[:exp].nil?
          expect(j.output_params).to be_nil
        else
          expect(j.output_params).to eq(x[:exp])
        end
      end
    end
  end
    
  describe "output params round trip" do
    [
      { inp: { foo: 'bar', quux: 1 }, exp: { foo: 'bar', quux: 1 } },
      { inp: { "foo" => :bar, "quux" => 1 }, exp: { foo: 'bar', quux: 1 } },
      { inp: nil, exp: nil},
      { inp: { foo: nil, quux: "" }, exp: { foo: nil, quux: '' } },
    ].each do |x|
      it x[:inp] do
        j = ETL::Model::Job.new
        j.output_params_hash = x[:inp]
        
        if x[:exp].nil?
          expect(j.output_params_hash).to be_nil
        else
          expect(j.output_params_hash).to eq(x[:exp])
        end
      end
    end
  end
    
  describe "input params round trip" do
    [
      { inp: { foo: 'bar', quux: 1 }, exp: { foo: 'bar', quux: 1 } },
      { inp: { "foo" => :bar, "quux" => 1 }, exp: { foo: 'bar', quux: 1 } },
      { inp: nil, exp: nil},
      { inp: { foo: nil, quux: "" }, exp: { foo: nil, quux: '' } },
    ].each do |x|
      it x[:inp] do
        j = ETL::Model::Job.new
        j.input_params_hash = x[:inp]
        
        if x[:exp].nil?
          expect(j.input_params_hash).to be_nil
        else
          expect(j.input_params_hash).to eq(x[:exp])
        end
      end
    end
  end
  
  
end
