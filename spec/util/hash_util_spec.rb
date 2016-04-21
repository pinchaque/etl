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
  
  it "stringifies keys" do

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
      "foo" => "1",
      "bar" => "2",
      "quu_uux" => {
        "embed" => {"embed3" => "6"},
        "embed2" => "5"
      },
      "array" => [
        "a",
        :b,
        # Doesn't symbolize within arrays
        {"c" => "x", :d => "y"}
      ]
    }
    
    expect(ETL::HashUtil.stringify_keys(orig)).to eq(expected)
  end
  
  it "sanitizes passwords" do
    orig = {
      "password" => "1",
      :password => "2",
      "passwd" => "2",
      :passwd => [1, 2, 3],
      "bar" => "quux",
      "foo" => {
        "embed" => {"password" => "6"},
        :passwd => "5"
      }
    }
    orig_dup = orig.dup
    
    expected = {
      "password" => "X",
      :password => "X",
      "passwd" => "X",
      :passwd => "X",
      "bar" => "quux",
      "foo" => {
        "embed" => {"password" => "X"},
        :passwd => "X"
      }
    }
    
    expect(ETL::HashUtil.sanitize(orig, "X")).to eq(expected)
    expect(orig).to eq(orig_dup) # didn't change original
  end
end
