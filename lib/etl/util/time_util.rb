module ETL::TimeUtil
  
  def self.round_year(t)
    ::Time.gm(t.year, 1, 1)
  end
  
  def self.round_quarter(t)
    ::Time.gm(t.year, (((t.month - 1) / 3) * 3) + 1, 1)
  end
  
  def self.round_month(t)
    ::Time.gm(t.year, t.month, 1)
  end
  
  def self.round_day(t)
    ::Time.gm(t.year, t.month, t.day)
  end
  
  def self.round_hour(t)
    ::Time.gm(t.year, t.month, t.day, t.hour, 0, 0)
  end
end
