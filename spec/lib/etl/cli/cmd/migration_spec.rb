require 'spec_helper'
require 'etl/cli/cmd/migration'
require 'json'

RSpec.describe ETL::Cli::Cmd::Migration::Create, skip: true do
  let(:described_instance) do
    instance = described_class.new('etl migration create', {})
    instance.parse(args)
    instance
  end

  let(:table) { "test_table" }
  let(:dir) { "#{Dir.pwd}/db" }
  let(:args) { ["--table", table, "--outputdir", dir, "--inputdir", dir] }
  let(:passwd) { "mysecretpassword" }

  before(:all) do
    Dir.mkdir 'db'
    f = File.open("#{Dir.pwd}/db/migration_config.yml", "w")
    s = <<-END
test_table:
  columns:
    day: day
    attribute: attr
END

    f << s
    f.close()
    system( "cp #{Dir.pwd}/etc/erb_templates/redshift_migration.erb #{Dir.pwd}/db/")
  end

  after(:all) do
    system( "rm -rf #{Dir.pwd}/db")
  end

  context 'schema_map to mysql' do
    before do
      # Run docker mysql
      system("docker run --name test-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=#{passwd} -d mysql:latest")
      sleep(0.5)
      allow(described_instance).to receive(:provider_params).and_return({ host: "172.17.0.1", adapter: "mysql2", database: "mysql", user: "root", password: passwd } )
    end

    after do
      system("docker stop test-mysql")
      system("docker rm test-mysql")
    end

    it "schema from mysql" do
      # Create test table
      described_instance.provider_connect.create_table! table do
        String :attribute, :size=>100 
        DateTime :day
      end

      expect( described_instance.source_schema ).to eq( [[:attribute, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"varchar(100)", :type=>:string, :ruby_default=>nil, :max_length=>100}],
                                                         [:day, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"datetime", :type=>:datetime, :ruby_default=>nil}]] )
    end
  end

  context 'schema_map to postgres' do
    before do
      # Run postgres docker
      system("docker run --name test-postgres -e POSTGRES_PASSWORD=#{passwd} -d postgres")
      sleep(0.5)
      allow(described_instance).to receive(:provider_params).and_return({ host: "localhost", adapter: "postgres", database: "postgres", user: "postgres", password: passwd } )
    end

    after do
      system("docker stop test-postgres")
      system("docker rm test-postgres")
    end

    it "schema from postgres" do
      # Create test table
      described_instance.provider_connect.create_table! table do
        String :attribute, :size=>100 
        DateTime :day
      end

      expect( described_instance.source_schema ).to eq( [[:attribute, {:oid=>1043, :db_type=>"character varying(100)", :default=>nil, :allow_null=>true, :primary_key=>false, :type=>:string, :ruby_default=>nil, :max_length=>100}],
                                                         [:day, {:oid=>1114, :db_type=>"timestamp without time zone", :default=>nil, :allow_null=>true, :primary_key=>false, :type=>:datetime, :ruby_default=>nil}]] )
    end
  end

  # Test with mysql format schema
  context 'with mysql' do
    before { allow(described_instance).to receive(:source_schema).and_return([[:attribute, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"varchar(100)", :type=>:string, :ruby_default=>nil, :max_length=>100}],
                                                                              [:day, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"datetime", :type=>:datetime, :ruby_default=>nil}]] ) }

    it '#schema_map' do
      expect( described_instance.schema_map ).to eq({ "day" => "datetime", "attr" => "varchar(100)" })
    end

    it '#up_sql' do
      expect( described_instance.up_sql.lstrip.rstrip ).to eq( "@client.execute(\"create table test_table ( day datetime, attr varchar(100) )\")" )
    end

    it '#down_sql' do
      expect( described_instance.down_sql.lstrip.rstrip ).to eq( "@client.execute(\"drop table test_table\")" )
    end

    it '#execute' do
      described_instance.execute
      expect(File).to exist("#{dir}/#{table}_0001.rb") 
    end

    it '#execute' do
      system( "rm #{dir}/db/*")
      described_instance.execute
      described_instance.execute
      expect(File).to exist("#{dir}/#{table}_0001.rb") 
      expect(File).to exist("#{dir}/#{table}_0002.rb") 
    end
  end

  # Test with postgres format schema
  context 'with postgres' do
    before { allow(described_instance).to receive(:source_schema).and_return([[:attribute, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"varchar(100)", :type=>:string, :ruby_default=>nil, :max_length=>100}],
                                                                              [:day, {:primary_key=>false, :allow_null=>true, :default=>nil, :db_type=>"datetime", :type=>:datetime, :ruby_default=>nil}]] ) }
    it '#schema_map' do
      expect( described_instance.schema_map ).to eq({ "day" => "datetime", "attr" => "varchar(100)" })
    end

    it '#up_sql' do
      expect( described_instance.up_sql.lstrip.rstrip ).to eq( "@client.execute(\"create table test_table ( day datetime, attr varchar(100) )\")" )
    end

    it '#down_sql' do
      expect( described_instance.down_sql.lstrip.rstrip ).to eq( "@client.execute(\"drop table test_table\")" )
    end
  end

  context 'with custom connection setting' do
    let(:args) { ['--provider', provider, '--host', host, '--user', user, '--password', password, '--database', database].concat(super()) }
    let(:provider) { 'provider' }
    let(:host) { 'localhost' }
    let(:user) { 'user' }
    let(:password) { 'password1234' }
    let(:database) { 'database' }
    it '#provider_params' do
      expect( described_instance.provider_params ).to eq( { host: host, adapter: provider, database: database, user: user, password: password } )
    end
  end

  context 'with missing custom connection setting' do
    let(:args) { ['--host', host, '--user', user, '--password', password, '--database', database].concat(super()) }
    let(:host) { 'localhost' }
    let(:user) { 'user' }
    let(:password) { 'password1234' }
    let(:database) { 'database' }
    it '#provider_params' do
      expect{ described_instance.provider_params }.to raise_error("source_db_params is not defined in the config file")
    end
  end
end
