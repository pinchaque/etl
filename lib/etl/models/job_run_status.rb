require 'sequel'

module ETL::Model
  class JobRunStatus < ::Sequel::Model

    # Returns the status ID given the label as a symbol
    def self.id_from_label(label)
      o = self.first(:label => label.to_s())
      o.nil? ? nil : o.id
    end

    # Returns the status label as a symbol given an ID
    def self.label_from_id(id)
      o = self.first(:id => id)
      o.nil? ? nil : o.label.to_sym()
    end
  end

  JobRunStatus.plugin :timestamps, :create => :created_at, :update => :updated_at, :update_on_create => true
end
