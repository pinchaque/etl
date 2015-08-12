RSpec.describe "hash_util" do

  it "symbolizes keys" do

    orig = {
      "foo" => "1",
      :bar => "2",
      "quu_uux" => {
        "embed" => {"embed3" => "6"},
        :embed2 => "5"
      },
      "array" => [
        "a",
        :b,
        {"c" => "x", :d => "y"}
      ]
    }

    expected = {
      :foo => "1",
      :bar => "2",
      :quu_uux => {
        :embed => {:embed3 => "6"},
        :embed2 => "5"
      },
      :array => [
        "a",
        :b,
        # Doesn't symbolize within arrays
        {"c" => "x", :d => "y"}
      ]
    }
    
    expect(ETL::HashUtil.symbolize_keys(orig)).to eq(expected)
  end
end
