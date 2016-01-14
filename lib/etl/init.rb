module ETL
  
  def ETL.logger
    @@logger ||= ETL.create_logger
  end
  
  def ETL.logger=(v)
    @@logger = v
  end
  
  def ETL.create_logger
    cfg = ETL.config.core[:log]
    Object::const_get(cfg[:class]).new(cfg)
  end
  
  def ETL.queue
    @@queue ||= ETL.create_queue
  end
  
  def ETL.queue=(v)
    @@queue = v
  end
  
  def ETL.create_queue
    cfg = ETL.config.core[:queue]
    Object::const_get(cfg[:class]).new(cfg)
  end  
end  
