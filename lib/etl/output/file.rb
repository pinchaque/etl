module ETL::Output

  class File < Base

    # Root directory under which all file-based data feeds will be placed
    # Directory structure is:
    # OUTPUT_ROOT/FEED_NAME/BATCH.(csv|json|xml)
    def output_root
      ETL.config.core[:job][:data_dir]
    end

    # Output file name for this batch
    def output_file
      [
        output_root,
        feed_name,
        batch_id + "." + output_extension
      ].join("/")
    end
  end
end
