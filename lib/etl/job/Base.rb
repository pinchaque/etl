
class Etl::Job::Base

  def run
    extract()
    transform()
    do_load()
  end


  def extract
  end

  def transform
  end

  def do_load
  end
end
