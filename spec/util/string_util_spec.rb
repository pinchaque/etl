RSpec.describe "snake_util" do

  describe "camel_to_snake_case" do
    d = {
      "Class::Foo" => "class::foo",
      "Class::FooBar" => "class::foo_bar",
      "FooBBar" => "foo_b_bar",
      "Foo1Bar" => "foo1_bar",
    }
    
    d.each do |inp, exp|
      it inp do
        expect(ETL::StringUtil.camel_to_snake(inp)).to eq(exp)
      end
    end
  end
end
