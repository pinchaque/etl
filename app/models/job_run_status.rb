class JobRunStatus < ActiveRecord::Base

  def id_from_label(label)
    o = this.find_by(label: label)
    if o.nil?
      return nil
    else
      return o.id
    end
  end
end
