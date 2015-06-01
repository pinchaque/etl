require 'etl/jobs/base.rb'

module ETL::Job

  class File < Base

    # Root directory under which all file-based data feeds will be placed
    # Directory structure is:
    # OUTPUT_ROOT/FEED_NAME/BATCH.(csv|json|xml)
    def output_root
      "/var/tmp/etl_test_output"
    end

    # Output file name for this batch
    def output_file(batch)
      [
        output_root,
        feed_name,
        batch.to_s() + "." + output_extension
      ].join("/")
    end
  end
end
