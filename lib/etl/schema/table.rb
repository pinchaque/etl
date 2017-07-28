
module ETL::Schema

  # Represents single data table including an ordered set of columns with
  # names and types.
  # columns: Hash of column name to ETL::Schema::Column objects
  # partition_columns: Hash of batch identifier to column name that is used
  #   for that partition; used for partition loads
  # primary_key: Array of columns that are primary keys; used for upsert
  #   and update loads
  class Table
    attr_accessor :columns, :partition_columns, :primary_key, :name, :like, :temp

    def initialize(name = "", opts = {})
      @columns = {}
      @partition_columns = {}
      @primary_key = []
      @name = name
      @like = opts.fetch(:like, '')
      @temp = opts.fetch(:temp, false)
    end

    def self.from_sequel_schema(schema)
      t = Table.new
      schema.each do |col|
        col_name = col[0]
        col_opts = col[1]

        # translate the database type from Sequel to our types
        type = case col_opts[:type]
        when :integer
          :int
        when :datetime
          :date
        when nil
          :string
        else
          col_opts[:type]
        end

        # TODO need to handle width and precision properly
        t.add_column(col_name, type, nil, nil)
      end
      return t
    end

    def self.from_redshift_schema(schema)
      t = Table.new
      schema.each do |col|
        col_name = col[0]
        col_opts = col[1]
        col_dkey = col[2]
        col_skey = col[3]

        # translate the database type from Sequel to our types
        type = case col_opts
        when "integer"
          :int
        when "datetime"
          :date
        when nil
          :string
        else
          col_opts.to_s
        end

        if col_dkey == 't'
          if col_skey == '1'
            add_sortkey(col_name)
          end
          add_distkey(col_name)
        end
        # TODO need to handle width and precision properly
        t.add_column(col_name, type, nil, nil)
      end
      return t
    end

    def to_s
      a = Array.new
      @columns.each do |k, v|
        a << "#{k.to_s} #{v.to_s}"
      end
      "(\n  " + a.join(",\n  ") + "\n)\n"
    end

    def add_column(name, type, width, precision, &block)
      raise ETL::SchemaError, "Invalid nil type for column '#{name}'" if type.nil?
      t = Column.new(type, width, precision)
      @columns[name.to_s] = t
      yield t if block_given?
    end

    def date(name, &block)
      add_column(name, :date, nil, nil, &block)
    end

    def defaultdate(name, dtype="GETDATE()", &block)
      sym = "datetime default #{dtype}".to_sym
      add_column(name, sym, nil, nil, &block)
    end

    def string(name, &block)
      add_column(name, :string, nil, nil, &block)
    end

    def int(name, &block)
      add_column(name, :int, nil, nil, &block)
    end

    def float(name, &block)
      add_column(name, :float, nil, nil, &block)
    end

    def text(name, &block)
      add_column(name, :text, nil, nil, &block)
    end

    def numeric(name, width, precision, &block)
      add_column(name, :numeric, width, precision, &block)
    end

    def boolean(name,  &block)
      add_column(name, :boolean, nil, nil, &block)
    end

    def varchar(name, range, &block)
      sym = "varchar (#{range})".to_sym
      add_column(name, sym, nil, nil, &block)
    end

    def add_primarykey(pks)
      @primary_key.push(pks)
    end

  end
end
