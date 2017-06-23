module ETL::Query

  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class Sequel
    attr_accessor :where, :group_by, :limit, :offset

    # select : list of strings
    # from : string
    # where : list of strings
    # group_by : list of strings
    # limit : int
    def initialize(select, from, where = nil, group_by = nil, limit = nil)
      if !select.is_a?(Array)
        raise "Select is not array"
      elsif !from.is_a?(String)
        raise "From is not string"
      elsif !where.nil? && !where.is_a?(String)
        raise "Where is not string"
      elsif !group_by.nil? && !group_by.is_a?(Array)
        raise "Group_by is not array"
      elsif !limit.nil? && !limit.is_a?(Integer)
        raise "Limit is not integer"
      end
        
      @select = select.delete_if(&:empty?)
      if @select.empty?
        raise "Select is empty"
      end

      if from.empty?
        raise "From is empty"
      end
      @from = from 
      @where = where if !where.nil? && !where.empty?
      @group_by = if group_by.nil?
                    group_by
                  else
                    group_by.delete_if(&:empty?)
                  end
      @limit = limit 
      @offset = nil
      @offset_startpoint  = 0
    end

    def query
      select = @select.join(", ")
      where = 
        if @where.nil? || @where.empty?
          if !@tmp_where.nil? && !@tmp_where.empty?
            " WHERE #{@tmp_where}"
          else
            ""
          end
        else
          if !@tmp_where.nil? && !@tmp_where.empty?
            " WHERE #{@where} #{@tmp_operator} #{@tmp_where}"
          else
            " WHERE #{@where}"
          end
        end
        
      group_by = 
        if @group_by.nil? || @group_by.empty?
          ""
        else
          " GROUP BY #{@group_by.join(", ")}"
        end

      limit =
        if @limit.nil?
          ""
        else
          " LIMIT #{@limit}"
        end

      if @offset.nil?
        offset = ""
      else
        if limit.empty?
          limit = " LIMIT #{@offset}"
        end
        offset = " OFFSET #{@offset_startpoint}"
      end

      "SELECT #{select} FROM #{@from}#{where}#{group_by}#{limit}#{offset}"
    end

    # where : string
    # operator : :AND or :OR 
    def append_where(where, operator = :AND)
      raise "Parameter is not Array" if !where.is_a?(String)
      raise "Invalid operator: #{operator}" if operator != :AND && operator != :OR

  	  @where = 
      if @where.nil? || @where.empty?
        where 
      else
        "#{@where} #{operator} #{where}"
      end
    end

    # parameter should be array
    def append_replaceable_where(where, operator = :AND)
      raise "Parameter is not string" if !where.is_a?(String)
      raise "Invalid operator: #{operator}" if operator != :AND && operator != :OR
      
      @tmp_where = where 
      @tmp_operator = operator 
    end

    def set_offset(offset)
      raise "Parameter is not integer" if !offset.is_a?(Integer)
      @offset = offset 
      @offset_startpoint += offset
    end

    def cancel_offset
      @offset = nil 
      @offset_startpoint = 0 
    end
  end
end
