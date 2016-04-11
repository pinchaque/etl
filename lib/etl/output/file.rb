module ETL::Output

  class File < Base
    
    def initialize(file_mode = :overwrite)
      super()
      @file_mode = file_mode
    end

    # Root directory under which all file-based data feeds will be placed
    # Directory structure is:
    # OUTPUT_ROOT/FEED_NAME/BATCH.(csv|json|xml)
    def output_root
      ETL.config.core[:job][:data_dir]
    end

    # Output file name for this batch
    def output_file
      raise "Invalid empty feed_name()" if feed_name.nil? || feed_name.empty?
      path = [
        output_root,
        feed_name,
        batch_id || "output"
      ].compact.join("/") + "." + output_extension
    end
  end
end
