RSpec.describe "batch_factory/base" do
  let(:bf) { ETL::BatchFactory::Base.new }
    
  describe "parses key=value strings" do
    {
      "" => { },
      "key1=value1" => { key1: "value1" },
      "key1=value1;key3=value3;key2=value2" => {
        key1: "value1", key2: "value2", key3: "value3" },
      "key_1=2016-03-01T10:11:12Z" => { key_1: "2016-03-01T10:11:12Z" },
      "k=v" => { k: "v" },
    }.each do |inp, exp|
      it inp do
        expect(bf.parse!(inp).to_h).to eq(exp)
      end
    end
  end

  describe "parses JSON strings" do
    {
      '{"foo":"bar","quux":123}' => { foo: "bar", quux: 123 },
    }.each do |inp, exp|
      it inp do
        expect(bf.parse(inp).to_h).to eq(exp)
      end
    end
  end
  
  describe "parsing expected failures" do
    {
      " " => "string with spaces",
      "key1 value 1" => "kv needs equals sign",
      "k1=v1;k2=v2;" => "cannot have dangling semicolon",
      "k1=v1;;k2=v2" => "cannot have extra internal semicolon",
      "key1=value 1" => "no spaces allowed in value",
      "key-1=value 1" => "no non-word chars allowed in key",
      "key:1=value 1" => "no non-word chars allowed in key",
    }.each do |inp, msg|
      it "#{msg}: '#{inp}'" do
        expect { bf.parse!(inp) }.to raise_error(ETL::BatchError)
        expect(bf.parse(inp)).to be_nil
      end
    end
  end
end
