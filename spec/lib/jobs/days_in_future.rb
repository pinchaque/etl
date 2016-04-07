module ETL::Test
  class DaysInFuture < ETL::Job::Base
    register_job
    
    def self.input_class
      Class.new(ETL::Input::Array) do
        def initialize(params = {})
          super( params.merge({
            data: (1..14).to_a.map { |x| { "num_days" => x } }
            } ) )
        end
      end
    end
    
    def self.output_class
      Class.new(ETL::Output::CSV) do
        def initialize(params = {})
          super(params.merge( { load_strategy: :insert_table } ))
          
          define_schema do |t|
            t.date(:date)
            t.int(:num_days)
            t.date(:future_date)
          end
        end
        
        # Needed because this is an anonymous class and therefore we can't 
        # derive a feed name.
        def feed_name
          'days_in_future'
        end
        
        # Adds computed columns
        def transform_row(row)
          row.merge({
            "date" => Time.now,
            "future_date" => Time.now + (row["num_days"] * 24 * 60 * 60),
          })
        end
      end
    end
    
    def batch_factory_class
      ETL::BatchFactory::Hour
    end
    
  end
end
