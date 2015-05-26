class JobRunStatus < ActiveRecord::Base

  # Returns the status ID given the label as a symbol
  def self.id_from_label(label)
    o = self.find_by!(label: 'new')
    if o.nil?
      return nil
    else
      return o.id
    end
  end

  # Returns the status label as a symbol given an ID
  def self.label_from_id(id)
    o = self.find(id)
    if o.nil?
      return nil
    else
      return o.label.to_sym()
    end
  end
end
