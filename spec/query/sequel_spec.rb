require 'etl/core'

RSpec.describe "sequel query" do

  let(:select) { ["foo"] }
  let(:from) { "bar" }
  let(:where) { "" }
  let(:group_by) { [] }
  let(:limit) { nil }
  let(:offset) { nil }
  let(:tmp_where) { "foo = foobar" }
  let(:tmp_appendwhere) { "foo != bar" }

  it "select - not array" do
    expect { ETL::Query::Sequel.new("foo", from, where, group_by, limit) }.to raise_error("Select is not array")
  end

  it "select - array of one string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "select - array of several strings" do
  	sequel_query = ETL::Query::Sequel.new(["foo", "oof"], from, where, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT foo, oof FROM #{from}")
  end

  it "select - array of an empty string" do
    expect { ETL::Query::Sequel.new([""], from, where, group_by, limit) }.to raise_error("Select is empty")
  end

  it "select - array of several empty strings" do
    expect { ETL::Query::Sequel.new(["", "", ""], from, where, group_by, limit) }.to raise_error("Select is empty")
  end

  it "from - not string" do
    expect { ETL::Query::Sequel.new(select, select, where, group_by, limit) }.to raise_error("From is not string")
  end

  it "from - empty string" do
    expect { ETL::Query::Sequel.new(select, "", where, group_by, limit) }.to raise_error("From is empty")
  end

  it "where - nil" do
  	sequel_query = ETL::Query::Sequel.new(select, from, nil, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "where - not string" do
    expect { ETL::Query::Sequel.new(select, from, [], group_by, limit) }.to raise_error("Where is not string")
  end

  it "where - string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, tmp_where, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_where}")
  end

  it "where - array of several empty strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, "", group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - nil" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, nil, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - not array" do
    expect { ETL::Query::Sequel.new(select, from, where, "", limit) }.to raise_error("Group_by is not array")
  end

  it "group_by - array of several strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, ["group", "by"], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} GROUP BY group, by")
  end

  it "group_by - array of an empty string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, [""], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - array of several empty strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, ["", "", ""], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "limit - int" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 1)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 1")
  end

  it "limit - not int" do
  	expect { ETL::Query::Sequel.new(select, from, where, group_by, "one") }.to raise_error("Limit is not integer")
  end

  it "offset - int without limit" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	sequel_query.set_offset(os)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT #{os} OFFSET #{os}")
  end

  it "offset - int with limit" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 20)
  	sequel_query.set_offset(os)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 20 OFFSET #{os}")
  end

  it "offset - move to next point" do
    os = 10
    sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 20)
    sequel_query.set_offset(os)
    sequel_query.set_offset(os)
    expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 20 OFFSET #{os*2}")
  end

  it "offset - cancel" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 20)
  	sequel_query.set_offset(os)
  	sequel_query.cancel_offset
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 20")
  end

  it "offset - not int" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.set_offset("ten") }.to raise_error("Parameter is not integer")
  end

  it "append_where - string without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	sequel_query.append_where(tmp_appendwhere)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_appendwhere}")
  end

  it "append_where - string with where and default operator" do
  	sequel_query = ETL::Query::Sequel.new(select, from, tmp_where, group_by, limit)
  	sequel_query.append_where(tmp_appendwhere)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_where} AND #{tmp_appendwhere}")
  end

  it "append_where - string with where and valid operator" do
  	sequel_query = ETL::Query::Sequel.new(select, from, tmp_where, group_by, limit)
  	sequel_query.append_where(tmp_appendwhere, :OR)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_where} OR #{tmp_appendwhere}")
  end

  it "append_where - invalid operator" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.append_where(tmp_appendwhere, "operator") }.to raise_error("Invalid operator: operator")
  end

  it "append_replaceable_where - a string without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)

  	sequel_query.append_replaceable_where(tmp_where)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_where}")

  	sequel_query.append_replaceable_where(tmp_appendwhere)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{tmp_appendwhere}")
  end

  it "append_replaceable_where - string with where" do
  	w = "bar = bar"
  	sequel_query = ETL::Query::Sequel.new(select, from, w, group_by, limit)

  	sequel_query.append_replaceable_where(tmp_where)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{w} AND #{tmp_where}")

  	sequel_query.append_replaceable_where(tmp_appendwhere)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{w} AND #{tmp_appendwhere}")

  	sequel_query.append_replaceable_where(tmp_where, :OR)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE #{w} OR #{tmp_where}")
  end

  it "append_replaceable_where - not string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.append_replaceable_where([tmp_appendwhere]) }.to raise_error("Parameter is not string")
  end

  it "append_replaceable_where - invalid operator" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.append_replaceable_where(tmp_appendwhere, "operator") }.to raise_error("Invalid operator: operator")
  end
end
