module ETL

  module Redshift

    # Represents single data table in redshift
    class Table < ETL::Schema::Table
      attr_accessor :backup, :dist_key, :sort_keys, :dist_style

      def initialize(name="", opts = {})
        super(name, opts)
        @dist_key = ""
        @sort_keys = []
        @backup = opts.fetch(:backup, true)
        @dist_style = opts.fetch(:dist_style, '')
      end

      def set_distkey(column)
        @dist_key = column
      end

      def add_sortkey(column)
        @sort_keys.push(column)
      end

      def create_table_sql
        temp =""
        temp = if @temp
                 " TEMPORARY"
               end


        sql = "CREATE#{temp} TABLE IF NOT EXISTS #{@name}"
        if !@like.empty?
          sql << " ( LIKE #{@like} )"
        end


        column_declare_statements = ""
        type_ary = []
        columns.each do |name, column|
          column_type = col_type_str(column)
          column_statement = "\"#{name}\" #{column_type}"
          column_statement += " NOT NULL" if @primary_key.include?(name.to_sym)

          type_ary << column_statement
        end

        if @primary_key.length > 0
          pks = @primary_key.join(",")
          type_ary << "PRIMARY KEY(#{pks})"
        end

        if type_ary.length > 0
          sql << "( #{type_ary.join(', ')} )"
        end

        # backup is by default on if not specified
        if !@backup
                 sql << " BACKUP NO"
        end

        if !@dist_key.empty?
          sql << " DISTKEY(#{@dist_key})"
        end

        if @sort_keys.length > 0
          sks = @sort_keys.join(",")
          sql << " SORTKEY(#{sks})"
        end

        if !@dist_style.empty?
          sql << " DISTSTYLE #{@dist_style}"
        end

        sql
      end

      def drop_table_sql
        sql = <<SQL
DROP TABLE IF EXISTS #{@name}
SQL
      end

      # Returns string that can be used as the database type given the
      # ETL::Schema::Column object
      def col_type_str(col)
        case col.type
          when :string
            "varchar(255)"
          when :date
            "timestamp"
          when :numeric
            s = "numeric"
            if !col.width.nil? || !col.precision.nil?
              s += "("
              s += col.width.nil? ? "0" : col.width.to_s()
              if !col.precision.nil?
                s += ", #{col.precision}"
              end
              s += ")"
            end
            s
          else
            # Allow other types to just flow through, which gives us a simple
            # way of supporting columns that are coming in through db reflection
            # even if we don't know what they are.
            col.type.to_s
        end
      end
    end
  end
end

